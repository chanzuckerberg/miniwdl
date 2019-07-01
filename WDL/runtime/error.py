from WDL.Error import RuntimeError as _RuntimeError


class CommandError(_RuntimeError):
    pass


class Terminated(_RuntimeError):
    pass


class OutputError(_RuntimeError):
    pass


class TaskFailure(_RuntimeError):
    task_name: str
    task_id: str

    def __init__(self, task_name: str, task_id: str) -> None:
        super().__init__("task {} ({}) failed".format(task_name, task_id))
        self.task_name = task_name
        self.task_id = task_id
