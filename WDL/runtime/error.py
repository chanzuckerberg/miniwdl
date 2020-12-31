# pyre-strict
from typing import Union, Dict, Any, Optional
from ..Error import RuntimeError as _RuntimeError, SourcePosition
from ..Tree import Task as _Task, Workflow as _Workflow
from . import _statusbar


class CommandFailed(_RuntimeError):
    """
    Failure of the task command
    """

    exit_status: int
    """
    Task command exit status
    """

    stderr_file: str
    """
    Path to a file containing the task's standard error
    """

    def __init__(self, exit_status: int, stderr_file: str, message: str = "") -> None:
        oom_hint = ", a possible indication that it ran out of memory" if exit_status == 137 else ""
        super().__init__(message or f"task command failed with exit status {exit_status}{oom_hint}")
        self.exit_status = exit_status
        self.stderr_file = stderr_file


class Terminated(_RuntimeError):
    """
    Workflow/task was intentionally terminated, e.g. by Unix signal
    """

    quiet: bool
    """
    Termination warrants less logging because it was a secondary side-effect of a previous error
    """

    def __init__(self, quiet: bool = False) -> None:
        self.quiet = quiet
        _statusbar.abort()


class Interrupted(_RuntimeError):
    """
    Task was interrupted by an exogenous problem (e.g. worker node went down)
    """

    pass


class OutputError(_RuntimeError):
    """
    Failure whilst gathering task outputs
    """

    pass


class DownloadFailed(_RuntimeError):
    """
    Failure to download a URI input file
    """

    uri: str

    def __init__(self, uri: str, message: str = "") -> None:
        super().__init__(message or ("unable to download " + uri))
        self.uri = uri
        _statusbar.abort()


class RunFailed(_RuntimeError):
    """"""

    exe: Union[_Task, _Workflow]
    run_id: str
    run_dir: str

    def __init__(self, exe: Union[_Task, _Workflow], run_id: str, run_dir: str) -> None:
        super().__init__(
            f"{'task' if isinstance(exe, _Task) else 'workflow'} {exe.name} "
            f"({exe.pos.uri} Ln {exe.pos.line} Col {exe.pos.column}) failed"
        )
        self.exe = exe
        self.run_id = run_id
        self.run_dir = run_dir
        _statusbar.abort()


def error_json(exn: BaseException, cause: Optional[Exception] = None) -> Dict[str, Any]:
    """
    Make a json-dumpable dict to write into error sentinel file
    """

    def pos_json(pos: SourcePosition) -> Dict[str, Any]:
        return {"source": pos.abspath, "line": pos.line, "column": pos.column}

    info: Dict[str, Any] = {"error": exn.__class__.__name__}
    pos = None
    from_pos = None
    if isinstance(exn, RunFailed):
        exe = getattr(exn, "exe")
        pos = getattr(exe, "pos")
        info["task" if isinstance(exe, _Task) else "workflow"] = exe.name
        info["run"] = getattr(exn, "run_id")
        info["dir"] = getattr(exn, "run_dir")
        # follow __cause__s to find the original triggering exception
        from_exn = exn
        from_run = None
        from_dir = dir
        while isinstance(from_exn, RunFailed):
            from_dir = getattr(from_exn, "run_dir")
            from_run = getattr(from_exn, "run_id")
            from_pos = getattr(from_exn, "pos", None) or getattr(getattr(from_exn, "exe"), "pos")
            from_exn = cause or from_exn.__cause__
            cause = None
        if from_exn and from_exn is not exn:
            info["cause"] = error_json(from_exn)
            info["cause"]["run"] = from_run
            info["cause"]["dir"] = from_dir
            info["cause"]["pos"] = pos_json(from_pos)
    elif isinstance(exn, CommandFailed):
        info["exit_status"] = getattr(exn, "exit_status")
        info["stderr_file"] = getattr(exn, "stderr_file")
    elif str(exn):
        info["message"] = str(exn)
    if hasattr(exn, "job_id"):
        info["node"] = getattr(exn, "job_id")
    pos = pos or getattr(exn, "pos", None)
    if isinstance(pos, SourcePosition):
        info["pos"] = pos_json(pos)
    return info
