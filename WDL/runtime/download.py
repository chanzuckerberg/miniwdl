"""
Downloading input files from URIs, with plugin modules for different URI schemes

Download URI plugins are installed & registered using the setuptools entry point group
"miniwdl.plugin.file_download", with name equal to the URI scheme (e.g. "gs" or "s3").

The plugin entry point should be a context manager, which the runtime keeps open for the duration of
the download operation. Given the desired URI, it should quickly yield a tuple with:
    1. source code of a WDL 1.0 task to perform the download
    2. dict of Cromwell-style JSON inputs to give to the task
miniwdl then executes this specified operation, expecting it to produce an output "File file" with
the downloaded file. By doing the heavy lifting in a WDL task, the operation gets to inherit all
the functionality of miniwdl's task runtime, e.g. pulling docker image with binary dependencies,
resource scheduling & isolation, logging, error/signal handling, retry, etc.
The Python context manager itself might be used to obtain and manage the lifetime of any needed
security credentials.
"""
import os
import logging
import traceback
import importlib_metadata
from typing import Optional, List, Generator, Dict, Any, Tuple, Callable
from . import config
from .cache import CallCache
from .._util import compose_coroutines
from .._util import StructuredLogMessage as _

# WDL tasks for downloading a file based on its URI scheme


def aria2c_downloader(
    cfg: config.Loader, logger: logging.Logger, uri: str, **kwargs
) -> Generator[Dict[str, Any], Dict[str, Any], None]:
    wdl = r"""
    task aria2c {
        input {
            String uri
            Int connections = 10
        }
        command <<<
            set -euxo pipefail
            mkdir __out
            cd __out
            aria2c -x ~{connections} -s ~{connections} \
                --file-allocation=none --retry-wait=2 --stderr=true --enable-color=false \
                "~{uri}"
        >>>
        output {
            File file = glob("__out/*")[0]
        }
        runtime {
            cpu: 4
            memory: "1G"
            docker: "hobbsau/aria2"
        }
    }
    """
    recv = yield {"task_wdl": wdl, "inputs": {"uri": uri}}
    yield recv  # pyre-ignore


def _load(cfg: config.Loader):
    table = getattr(cfg, "_downloaders", None)
    if table:
        return table

    # default public URI downloaders
    table = {"https": aria2c_downloader, "http": aria2c_downloader, "ftp": aria2c_downloader}

    # plugins
    for plugin_name, plugin_fn in config.load_plugins(cfg, "file_download"):
        table[plugin_name] = plugin_fn

    setattr(cfg, "_downloaders", table)
    return table


def _downloader(
    cfg: config.Loader, uri: str,
) -> Optional[Callable[..., Generator[Dict[str, Any], Dict[str, Any], None]]]:
    _load(cfg)
    colon = uri.find(":")
    if colon <= 0:
        return None
    scheme = uri[:colon]
    return getattr(cfg, "_downloaders").get(scheme, None)


def able(cfg: config.Loader, uri: str) -> bool:
    """
    Returns True if uri appears to be a URI we know how to download
    """
    return _downloader(cfg, uri) is not None


def run(cfg: config.Loader, logger: logging.Logger, uri: str, **kwargs) -> str:
    """
    Download the URI and return the local filename.

    kwargs are passed through to ``run_local_task``, so ``run_dir`` and ``logger_prefix`` may be
    useful in particular.
    """

    from . import run_local_task, RunFailed, DownloadFailed, Terminated, error_json
    from .. import parse_tasks, values_from_json, values_to_json

    gen = _downloader(cfg, uri)
    assert gen
    try:
        with compose_coroutines([lambda kwargs: gen(cfg, logger, **kwargs)], {"uri": uri}) as cor:
            recv = next(cor)

            if "task_wdl" in recv:
                task_wdl, inputs = (recv[k] for k in ["task_wdl", "inputs"])

                task = parse_tasks(task_wdl, version="1.0")[0]  # pyre-ignore
                task.typecheck()
                inputs = values_from_json(inputs, task.available_inputs)  # pyre-ignore
                subdir, outputs_env = run_local_task(
                    cfg, task, inputs, run_id=("download-" + task.name), **kwargs
                )

                recv = cor.send(
                    {"outputs": values_to_json(outputs_env), "dir": subdir}  # pyre-ignore
                )

            ans = recv["outputs"]["file"]
            assert isinstance(ans, str) and os.path.isfile(ans)
            return ans

    except RunFailed as exn:
        if isinstance(exn.__cause__, Terminated):
            raise exn.__cause__ from None
        raise DownloadFailed(uri) from exn.__cause__
    except Exception as exn:
        logger.debug(traceback.format_exc())
        logger.error(_("downloader error", uri=uri, **error_json(exn)))
        raise DownloadFailed(uri) from exn


def run_cached(
    cfg, logger: logging.Logger, cache: CallCache, uri: str, run_dir: str, **kwargs
) -> Tuple[bool, str]:
    """
    Cached download logic: returns the file from the cache if available; otherwise, runs the
    download and puts it into the cache before returning
    """
    cached = cache.get_download(uri, logger=logger)
    if cached:
        return True, cached
    if not cfg["download_cache"].get_bool("put") or not cache.download_path(uri):
        return False, run(cfg, logger, uri, run_dir=run_dir, **kwargs)
    # run the download within the cache directory
    run_dir = os.path.join(cfg["download_cache"]["dir"], "ops")
    filename = run(cfg, logger, uri, run_dir=run_dir, **kwargs)
    return False, cache.put_download(uri, os.path.realpath(filename), logger=logger)


def gsutil_downloader(
    cfg: config.Loader, logger: logging.Logger, uri: str, **kwargs
) -> Generator[Dict[str, Any], Dict[str, Any], None]:
    """
    Built-in downloader plugin for public gs:// URIs; registered by setup.cfg entry_points section

    TODO: adopt security credentials from runtime environment
    """
    if uri == "gs://8675309":
        # hook for test coverage of exception handler
        raise RuntimeError("don't change your number")
    wdl = r"""
    task gsutil_cp {
        input {
            String uri
        }
        command <<<
            set -euxo pipefail
            mkdir __out/
            gsutil -q cp "~{uri}" __out/
        >>>
        output {
            File file = glob("__out/*")[0]
        }
        runtime {
            cpu: 4
            memory: "1G"
            docker: "google/cloud-sdk:slim"
        }
    }
    """
    yield (yield {"task_wdl": wdl, "inputs": {"uri": uri}})  # pyre-ignore
