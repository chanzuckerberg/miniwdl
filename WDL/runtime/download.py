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
import tempfile
import hashlib
import importlib_metadata
from contextlib import ExitStack
from typing import Optional, List, Generator, Dict, Any, Tuple, Callable
from . import config
from .cache import CallCache
from .._util import compose_coroutines
from .._util import StructuredLogMessage as _


def _load(cfg: config.Loader):
    table = getattr(cfg, "_downloaders", None)
    if table:
        return table

    # default public URI downloaders
    table = {
        "https": aria2c_downloader,
        "http": aria2c_downloader,
        "ftp": aria2c_downloader,
        "s3": awscli_downloader,
    }

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

    from .error import RunFailed, DownloadFailed, Terminated, error_json
    from .task import run_local_task
    from .. import parse_document, values_from_json, values_to_json, Walker

    gen = _downloader(cfg, uri)
    assert gen
    try:
        with compose_coroutines([lambda kwargs: gen(cfg, logger, **kwargs)], {"uri": uri}) as cor:
            recv = next(cor)

            if "task_wdl" in recv:
                task_wdl, inputs = (recv[k] for k in ["task_wdl", "inputs"])

                doc = parse_document(task_wdl, version="1.0")  # pyre-ignore
                assert len(doc.tasks) == 1 and not doc.workflow
                doc.typecheck()
                Walker.SetParents()(doc)
                task = doc.tasks[0]
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
    if not cache.download_cacheable(uri):
        return False, run(cfg, logger, uri, run_dir=run_dir, **kwargs)
    # run the download within the cache directory
    run_dir = os.path.join(cfg["download_cache"]["dir"], "ops")
    filename = run(cfg, logger, uri, run_dir=run_dir, **kwargs)
    return False, cache.put_download(uri, os.path.realpath(filename), logger=logger)


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


def awscli_downloader(
    cfg: config.Loader, logger: logging.Logger, uri: str, **kwargs
) -> Generator[Dict[str, Any], Dict[str, Any], None]:

    # get AWS credentials from boto3 (unless prevented by configuration)
    host_aws_credentials = None
    if cfg["download_awscli"].get_bool("host_credentials"):
        try:
            import boto3  # pyre-fixme

            b3creds = boto3.session.Session().get_credentials()
            host_aws_credentials = "\n".join(
                f"export {k}='{v}'"
                for (k, v) in {
                    "AWS_ACCESS_KEY_ID": b3creds.access_key,
                    "AWS_SECRET_ACCESS_KEY": b3creds.secret_key,
                    "AWS_SESSION_TOKEN": b3creds.token,
                }.items()
                if v
            )
        except Exception:
            pass

    inputs = {"uri": uri}
    with ExitStack() as cleanup:
        if host_aws_credentials:
            # write credentials to temp file that'll self-destruct afterwards
            aws_credentials_file = cleanup.enter_context(
                tempfile.NamedTemporaryFile(
                    prefix=hashlib.sha256(host_aws_credentials.encode()).hexdigest(),
                    delete=True,
                    mode="w",
                )
            )
            print(host_aws_credentials, file=aws_credentials_file, flush=True)
            # make file group-readable to ensure it'll be usable if the docker image runs as non-root
            os.chmod(aws_credentials_file.name, os.stat(aws_credentials_file.name).st_mode | 0o40)
            inputs["aws_credentials"] = aws_credentials_file.name
            logger.getChild("awscli_downloader").info("loaded host AWS credentials")
        else:
            logger.getChild("awscli_downloader").info(
                "no AWS credentials available via host awscli/boto3; if needed, "
                "configure them and set [download_awscli] host_credentials=true. "
                "(On EC2: awscli might still assume role from instance metadata "
                "service.)"
            )

        wdl = r"""
        task aws_s3_cp {
            input {
                String uri
                File? aws_credentials
            }

            command <<<
                set -euo pipefail
                if [ -n "~{aws_credentials}" ]; then
                    source "~{aws_credentials}"
                fi
                args=""
                if ! aws sts get-caller-identity >&2 ; then
                    # no credentials or instance role; add --no-sign-request to allow requests for
                    # PUBLIC objects to proceed.
                    args="--no-sign-request"
                fi
                mkdir __out
                cd __out
                aws s3 cp $args "~{uri}" .
            >>>

            output {
                File file = glob("__out/*")[0]
            }

            runtime {
                cpu: 4
                memory: "1G"
                docker: "amazon/aws-cli"
            }
        }
        """
        recv = yield {
            "task_wdl": wdl,
            "inputs": inputs,
        }
    yield recv  # pyre-ignore


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
