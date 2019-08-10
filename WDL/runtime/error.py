# pyre-strict
from ..Error import RuntimeError as _RuntimeError
from ..Tree import Task as _Task


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

    task: _Task
    run_id: str
    run_dir: str

    def __init__(self, task: _Task, run_id: str, run_dir: str) -> None:
        super().__init__(
            f"task {task.name} ({task.pos.uri} Ln {task.pos.line} Col {task.pos.column}) failed"
        )
        self.task = task
        self.run_id = run_id
        self.run_dir = run_dir
