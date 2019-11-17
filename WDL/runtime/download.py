"""
Logic for downloading input files from URIs
"""
import os
from typing import Optional, List, Iterable, Dict

# WDL tasks for downloading a file based on its URI scheme
# TODO: formalize plugin mechanism

_downloaders = {}
_downloaders[
    "https"
] = r"""
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
_downloaders["http"] = _downloaders["https"]
_downloaders["ftp"] = _downloaders["https"]


def _downloader(uri: str) -> Optional[str]:
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


def run(uri: str, **kwargs) -> str:
    """
    Download the URI and return the local filename.

    kwargs are passed through to ``run_local_task``, so ``run_dir`` and ``logger_prefix`` may be
    useful in particular.
    """
    from . import run_local_task, RunFailed, DownloadFailed
    from .. import parse_tasks, values_from_json

    downloader_wdl = _downloader(uri)
    assert downloader_wdl

    task = parse_tasks(downloader_wdl, version="1.0")[0]  # pyre-ignore
    task.typecheck()
    inputs = values_from_json({"uri": uri}, task.available_inputs)  # pyre-ignore
    try:
        subdir, outputs = run_local_task(task, inputs, **kwargs)
    except RunFailed as exn:
        raise DownloadFailed(uri) from exn.__cause__
    return outputs["file"].value
