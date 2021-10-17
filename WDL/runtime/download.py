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
import shlex
from contextlib import ExitStack
from typing import Optional, Generator, Dict, Any, Tuple, Callable
from . import config
from .cache import CallCache
from .._util import compose_coroutines
from .._util import StructuredLogMessage as _


def _load(cfg: config.Loader):
    if getattr(cfg, "_downloaders", None):
        return

    # default public URI downloaders
    file_downloaders = {
        "https": aria2c_downloader,
        "http": aria2c_downloader,
        "ftp": aria2c_downloader,
    }
    directory_downloaders = {}

    # plugins
    for plugin_name, plugin_fn in config.load_plugins(cfg, "file_download"):
        file_downloaders[plugin_name] = plugin_fn
    for plugin_name, plugin_fn in config.load_plugins(cfg, "directory_download"):
        directory_downloaders[plugin_name] = plugin_fn

    setattr(cfg, "_downloaders", (file_downloaders, directory_downloaders))


def _downloader(
    cfg: config.Loader, uri: str, directory: bool = False
) -> Optional[Callable[..., Generator[Dict[str, Any], Dict[str, Any], None]]]:
    _load(cfg)
    colon = uri.find(":")
    if colon <= 0:
        return None
    scheme = uri[:colon]
    return getattr(cfg, "_downloaders")[1 if directory else 0].get(scheme, None)


def able(cfg: config.Loader, uri: Optional[str], directory: bool = False) -> bool:
    """
    Returns True if uri appears to be a URI we know how to download
    """
    return bool(uri and _downloader(cfg, uri, directory=directory) is not None)


def run(
    cfg: config.Loader, logger: logging.Logger, uri: str, directory: bool = False, **kwargs
) -> str:
    """
    Download the URI and return the local filename.

    kwargs are passed through to ``run_local_task``, so ``run_dir`` and ``logger_prefix`` may be
    useful in particular.
    """

    from .error import RunFailed, DownloadFailed, Terminated, error_json
    from .task import run_local_task
    from .. import parse_document, values_from_json, values_to_json, Walker

    gen = _downloader(cfg, uri, directory=directory)
    assert gen
    try:
        logger.info(_(f"start {'directory ' if directory else ''}download", uri=uri))
        with compose_coroutines([lambda kwargs: gen(cfg, logger, **kwargs)], {"uri": uri}) as cor:
            recv = next(cor)

            if "task_wdl" in recv:
                task_wdl, inputs = (recv[k] for k in ["task_wdl", "inputs"])

                doc = parse_document(task_wdl, version="development")  # pyre-ignore
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

            ans = recv["outputs"]["directory" if directory else "file"]
            assert isinstance(ans, str) and os.path.exists(ans)
            logger.info(_(f"downloaded{' directory' if directory else ' file'}", uri=uri, file=ans))
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
    cfg,
    logger: logging.Logger,
    cache: CallCache,
    uri: str,
    run_dir: str,
    directory: bool = False,
    **kwargs,
) -> Tuple[bool, str]:
    """
    Cached download logic: returns the file from the cache if available; otherwise, runs the
    download and puts it into the cache before returning
    """
    cached = cache.get_download(uri, directory=directory, logger=logger)
    if cached:
        return True, cached
    cache_path = cache.download_cacheable(uri, directory=directory)
    cache_path_preexists = cache_path and os.path.exists(cache_path)
    if cache_path and not cache_path_preexists:
        # run the download in a holding area under the cache directory, so that it can later be
        # moved atomically into its final cache location
        run_dir = os.path.join(cfg["download_cache"]["dir"], "ops")
    filename = run(cfg, logger, uri, directory=directory, run_dir=run_dir, **kwargs)
    if cache_path_preexists:
        # a cache entry had already existed, but we didn't use it (--no-cache).
        # FIXME: it'd be better to replace the old copy...but what if another workflow is using it?
        logger.warning(
            _(
                "ignored a previously-cached download, which remains in the cache",
                uri=uri,
                downloaded=filename,
                cache_path=cache_path,
            )
        )
        # use the newly downloaded copy in the current run directory
        cache.memo_download(uri, filename, directory=directory)
        return False, filename
    return False, cache.put_download(
        uri, os.path.realpath(filename), directory=directory, logger=logger
    )


# WDL tasks for downloading a file based on its URI scheme


def aria2c_downloader(
    cfg: config.Loader, logger: logging.Logger, uri: str, **kwargs
) -> Generator[Dict[str, Any], Dict[str, Any], None]:
    wdl = r"""
    task aria2c {
        input {
            String uri
            String docker
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
            cpu: 2
            memory: "1G"
            docker: docker
        }
    }
    """
    recv = yield {
        "task_wdl": wdl,
        "inputs": {"uri": uri, "docker": cfg["download_aria2c"]["docker"]},
    }
    yield recv  # pyre-ignore


def awscli_downloader(
    cfg: config.Loader, logger: logging.Logger, uri: str, **kwargs
) -> Generator[Dict[str, Any], Dict[str, Any], None]:
    inputs: Dict[str, Any] = {"uri": uri, "docker": cfg["download_awscli"]["docker"]}
    with ExitStack() as cleanup:
        inputs["aws_credentials"] = prepare_aws_credentials(cfg, logger, cleanup)

        wdl = r"""
        task aws_s3_cp {
            input {
                String uri
                String docker
                File? aws_credentials
            }

            command <<<
                set -euo pipefail
                if [ -n "~{aws_credentials}" ]; then
                    source "~{aws_credentials}"
                fi
                export AWS_RETRY_MODE=standard
                export AWS_MAX_ATTEMPTS=5
                set -x
                mkdir __out
                if ! aws s3 cp "~{uri}" __out/ ; then
                    # Retry with --no-sign-request in case the object is public. Without this flag,
                    # the previous invocation could have failed either because (i) no AWS
                    # credentials are available or (ii) the IAM policy restricts accessible S3
                    # buckets regardless of whether the desired object is public.
                    rm -f __out/*
                    >&2 echo 'Retrying with --no-sign-request in case the object is public.' \
                         ' If the overall operation fails, the real error may precede this message.'
                    aws s3 cp --no-sign-request "~{uri}" __out/
                fi
            >>>

            output {
                File file = glob("__out/*")[0]
            }

            runtime {
                cpu: 2
                memory: "1G"
                docker: docker
            }
        }
        """
        recv = yield {"task_wdl": wdl, "inputs": inputs}
    yield recv  # pyre-ignore


def awscli_directory_downloader(
    cfg: config.Loader, logger: logging.Logger, uri: str, **kwargs
) -> Generator[Dict[str, Any], Dict[str, Any], None]:
    inputs: Dict[str, Any] = {"uri": uri, "docker": cfg["download_awscli"]["docker"]}
    with ExitStack() as cleanup:
        inputs["aws_credentials"] = prepare_aws_credentials(cfg, logger, cleanup)

        wdl = r"""
        task aws_s3_cp_directory {
            input {
                String uri
                String docker
                File? aws_credentials
            }

            String dnm = basename(uri, "/")

            command <<<
                set -euo pipefail
                if [ -n "~{aws_credentials}" ]; then
                    source "~{aws_credentials}"
                fi
                export AWS_RETRY_MODE=standard
                export AWS_MAX_ATTEMPTS=5
                set -x
                mkdir -p "__out/~{dnm}/"
                if ! aws s3 cp --recursive "~{uri}" "__out/~{dnm}/" ; then
                    # Retry with --no-sign-request in case the object is public. Without this flag,
                    # the previous invocation could have failed either because (i) no AWS
                    # credentials are available or (ii) the IAM policy restricts accessible S3
                    # buckets regardless of whether the desired object is public.
                    rm -f "__out/~{dnm}/*"
                    >&2 echo 'Retrying with --no-sign-request in case the folder is public.' \
                        ' If the overall operation fails, the real error may precede this message.'
                    aws s3 cp --recursive --no-sign-request "~{uri}" "__out/~{dnm}/"
                fi
            >>>

            output {
                Directory directory = "__out/" + dnm
            }

            runtime {
                cpu: 2
                memory: "1G"
                docker: docker
            }
        }
        """
        recv = yield {"task_wdl": wdl, "inputs": inputs}
    yield recv  # pyre-ignore


def prepare_aws_credentials(
    cfg: config.Loader, logger: logging.Logger, cleanup: ExitStack
) -> Optional[str]:
    host_aws_credentials = {}
    if "AWS_EC2_METADATA_DISABLED" in os.environ:
        # https://github.com/aws/aws-cli/issues/5623
        host_aws_credentials["AWS_EC2_METADATA_DISABLED"] = os.environ["AWS_EC2_METADATA_DISABLED"]
    # get AWS credentials from boto3 (unless prevented by configuration)
    if cfg["download_awscli"].get_bool("host_credentials"):
        import boto3  # pyre-fixme

        try:
            b3creds = boto3.session.Session().get_credentials()
            host_aws_credentials["AWS_ACCESS_KEY_ID"] = b3creds.access_key
            host_aws_credentials["AWS_SECRET_ACCESS_KEY"] = b3creds.secret_key
            host_aws_credentials["AWS_SESSION_TOKEN"] = b3creds.token
        except Exception:
            pass

    if host_aws_credentials:
        # write credentials to temp file that'll self-destruct afterwards
        host_aws_credentials = (
            "\n".join(f"export {k}={shlex.quote(v)}" for (k, v) in host_aws_credentials.items())
            + "\n"
        )
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
        logger.getChild("awscli_downloader").info("loaded host AWS credentials")
        return aws_credentials_file.name
    else:
        logger.getChild("awscli_downloader").info(
            "no AWS credentials available via host awscli/boto3; if needed, "
            "configure them and set [download_awscli] host_credentials=true. "
            "(On EC2: awscli might still assume role from instance metadata "
            "service.)"
        )
        return None


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
            String docker
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
            cpu: 2
            memory: "1G"
            docker: docker
        }
    }
    """
    yield (  # pyre-ignore
        yield {"task_wdl": wdl, "inputs": {"uri": uri, "docker": cfg["download_gsutil"]["docker"]}}
    )
