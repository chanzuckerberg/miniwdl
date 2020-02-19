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
import importlib_metadata
from typing import Optional, List, Iterable, Iterator, Dict, Any, Tuple, ContextManager, Callable
from contextlib import contextmanager
from . import config
from .cache import CallCache

# WDL tasks for downloading a file based on its URI scheme


_downloaders = {}


@contextmanager
def aria2c_downloader(
    cfg: config.Loader, logger: logging.Logger, uri: str
) -> Iterator[Tuple[str, Dict[str, Any]]]:
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
    yield wdl, {"uri": uri}


def _load():
    if _downloaders:
        return

    # default public URI downloaders
    _downloaders["https"] = aria2c_downloader
    _downloaders["http"] = aria2c_downloader
    _downloaders["ftp"] = aria2c_downloader

    # plugins
    for plugin in importlib_metadata.entry_points().get("miniwdl.plugin.file_download", []):
        _downloaders[plugin.name] = plugin.load()


def _downloader(
    uri: str,
) -> Optional[
    Callable[[config.Loader, logging.Logger, str], ContextManager[Tuple[str, Dict[str, Any]]]]
]:
    _load()
    colon = uri.find(":")
    if colon <= 0:
        return None
    scheme = uri[:colon]
    return _downloaders.get(scheme, None)


def able(uri: str) -> bool:
    """
    Returns True if uri appears to be a URI we know how to download
    """
    return _downloader(uri) is not None


def run(cfg: config.Loader, logger: logging.Logger, uri: str, **kwargs) -> str:
    """
    Download the URI and return the local filename.

    kwargs are passed through to ``run_local_task``, so ``run_dir`` and ``logger_prefix`` may be
    useful in particular.
    """

    from . import run_local_task, RunFailed, DownloadFailed, Terminated
    from .. import parse_tasks, values_from_json

    downloader_ctx = _downloader(uri)
    assert downloader_ctx
    try:
        with downloader_ctx(cfg, logger, uri) as (downloader_wdl, downloader_inputs):
            task = parse_tasks(downloader_wdl, version="1.0")[0]  # pyre-ignore
            task.typecheck()
            inputs = values_from_json(downloader_inputs, task.available_inputs)  # pyre-ignore
            subdir, outputs = run_local_task(cfg, task, inputs, **kwargs)
            return outputs["file"].value
    except RunFailed as exn:
        if isinstance(exn.__cause__, Terminated):
            raise exn.__cause__ from None
        raise DownloadFailed(uri) from exn.__cause__
    except:
        raise DownloadFailed(uri)


def run_cached(
    cfg, logger: logging.Logger, cache: CallCache, uri: str, run_dir: str, **kwargs
) -> Tuple[bool, str]:
    """
    Cached download logic: returns the file from the cache if available; otherwise, runs the
    download and puts it into the cache before returning
    """
    cached = cache.get_download(logger, uri)
    if cached:
        return True, cached
    if not cfg["download_cache"].get_bool("put") or not cache.download_path(uri):
        return False, run(cfg, logger, uri, run_dir=run_dir, **kwargs)
    # run the download within the cache directory
    run_dir = os.path.join(cfg["download_cache"]["dir"], "ops")
    filename = run(cfg, logger, uri, run_dir=run_dir, **kwargs)
    return False, cache.put_download(logger, uri, os.path.realpath(filename))


@contextmanager
def gsutil_downloader(
    cfg: config.Loader, logger: logging.Logger, uri: str
) -> Iterator[Tuple[str, Dict[str, Any]]]:
    """
    Built-in downloader plugin for public gs:// URIs; registered by setup.cfg entry_points section

    TODO: adopt security credentials from runtime environment
    """
    yield r"""
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
    """, {
        "uri": uri
    }
