"""
Logic for downloading input files from URIs
"""
import os
from typing import Optional, List, Iterable, Iterator, Dict, Any, Tuple, ContextManager, Callable
from contextlib import contextmanager
from reentry import manager as entry_pt_manager

# WDL tasks for downloading a file based on its URI scheme


@contextmanager
def aria2c_downloader(uri: str) -> Iterator[Tuple[str, Dict[str, Any]]]:
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


@contextmanager
def gsutil_downloader(uri: str) -> Iterator[Tuple[str, Dict[str, Any]]]:
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
    yield wdl, {"uri": uri}


_downloaders = {}


def _load():
    if _downloaders:
        return

    # default public URI downloaders
    _downloaders["https"] = aria2c_downloader
    _downloaders["http"] = aria2c_downloader
    _downloaders["ftp"] = aria2c_downloader
    _downloaders["gs"] = gsutil_downloader

    # plugins
    for plugin in entry_pt_manager.iter_entry_points(group="miniwdl.plugin.file_download"):
        _downloaders[plugin.name] = plugin.load()


def _downloader(uri: str,) -> Optional[Callable[[str], ContextManager[Tuple[str, Dict[str, Any]]]]]:
    _load()
    colon = uri.find(":")
    if colon <= 0:
        return None
    scheme = uri[:colon]
    return _downloaders.get(scheme, None)


def schemes() -> Iterable[str]:
    _load()
    yield from _downloaders.keys()


def able(uri: str) -> bool:
    """
    Returns True if uri appears to be a URI we know how to download
    """
    return _downloader(uri) is not None


def run(uri: str, **kwargs) -> str:
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
        with downloader_ctx(uri) as (downloader_wdl, downloader_inputs):
            task = parse_tasks(downloader_wdl, version="1.0")[0]  # pyre-ignore
            task.typecheck()
            inputs = values_from_json(downloader_inputs, task.available_inputs)  # pyre-ignore
            subdir, outputs = run_local_task(task, inputs, **kwargs)
            return outputs["file"].value
    except RunFailed as exn:
        if isinstance(exn.__cause__, Terminated):
            raise exn.__cause__ from None
        raise DownloadFailed(uri) from exn.__cause__
    except:
        raise DownloadFailed(uri)
