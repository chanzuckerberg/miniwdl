from typing import Optional, List

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
            --file-allocation=none --retry-wait=2 --stderr=true \
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
    return _downloader(uri) is not None


def run(uri: str, dir: str, logger_prefix: Optional[List[str]] = None) -> str:
    from . import run_local_task, DownloadFailed, RunFailed
    from .. import parse_tasks, values_from_json

    downloader_wdl = _downloader(uri)
    if not downloader_wdl:
        raise DownloadFailed(uri, "no downloader for scheme " + uri)
    task = parse_tasks(downloader_wdl, version="1.0")[0]  # pyre-ignore
    task.typecheck()
    inputs = values_from_json({"uri": uri}, task.available_inputs)  # pyre-ignore
    try:
        subdir, outputs = run_local_task(task, inputs, run_dir=dir, logger_prefix=logger_prefix)
    except RunFailed as exn:
        raise DownloadFailed(uri, str(exn.__cause__))
    return outputs["file"].value
