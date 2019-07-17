# pyre-strict
from typing import Optional
from ..Error import RuntimeError as _RuntimeError


class CommandFailure(_RuntimeError):
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


class TaskFailure(_RuntimeError):
    """

    """

    task_name: str
    run_id: str
    run_dir: str

    def __init__(self, task_name: str, run_id: str, run_dir: str) -> None:
        super().__init__(f"task {run_id} failed")
        self.task_name = task_name
        self.run_id = run_id
        self.run_dir = run_dir
