from enum import Enum


class TaskStatus(Enum):
    """
    Allows us to track the status of a task.

    Ideally (if no errors occur), the status should go from "pending" to "finished", as follows:
        - "pending"
        - "started"
        - "finished"
    """
    PENDING = "pending"
    STARTED = "started"
    FINISHED = "finished"
    FAILED = "failed"


class Task:
    def __init__(self, index_name: str) -> None:
        self.index_name = index_name
        self.status: TaskStatus = TaskStatus.PENDING

    def is_pending(self) -> bool:
        return self.status == TaskStatus.PENDING

    def is_started(self) -> bool:
        return self.status == TaskStatus.STARTED

    def set_started(self) -> None:
        self.status = TaskStatus.STARTED

    def is_finished(self) -> bool:
        return self.status == TaskStatus.FINISHED

    def set_finished(self) -> None:
        self.status = TaskStatus.FINISHED


class TaskWithInterval(Task):
    def __init__(
        self,
        index_name: str,
        start_timestamp: int,
        end_timestamp: int
    ) -> None:
        super().__init__(index_name)
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp


class TaskWithoutInterval(Task):
    def __init__(self, index_name: str) -> None:
        super().__init__(index_name)
