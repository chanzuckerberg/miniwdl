# pyre-strict
from typing import Union
from ..Error import RuntimeError as _RuntimeError
from ..Tree import Task as _Task, Workflow as _Workflow


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
        super().__init__(message or f"task command failed with exit status {exit_status}")
        self.exit_status = exit_status
        self.stderr_file = stderr_file


class Terminated(_RuntimeError):
    """
    Workflow/task was terminated, e.g. by Unix signal
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


class RunFailed(_RuntimeError):
    """

    """

    exe: Union[_Task, _Workflow]
    run_id: str
    run_dir: str

    def __init__(self, exe: Union[_Task, _Workflow], run_id: str, run_dir: str) -> None:
        super().__init__(
            f"{'task' if  isinstance(exe, _Task) else 'workflow'} {exe.name} ({exe.pos.uri} Ln {exe.pos.line} Col {exe.pos.column}) failed"
        )
        self.exe = exe
        self.run_id = run_id
        self.run_dir = run_dir
