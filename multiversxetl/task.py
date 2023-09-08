import datetime
from enum import Enum
from typing import Any, Dict, Optional

INDEX_NAME_FORMAT_RJUST = 32


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
        self.error: Optional[Exception] = None
        self.error_stack_trace: str = ""

    def is_pending(self) -> bool:
        return self.status == TaskStatus.PENDING

    def is_started(self) -> bool:
        return self.status == TaskStatus.STARTED

    def set_started(self) -> None:
        assert self.is_pending()
        self.status = TaskStatus.STARTED

    def is_finished(self) -> bool:
        return self.status == TaskStatus.FINISHED

    def set_finished(self) -> None:
        assert self.is_started()
        self.status = TaskStatus.FINISHED

    def is_failed(self) -> bool:
        return self.status == TaskStatus.FAILED

    def set_failed(self, error: Exception, formatted_stack_trace: str) -> None:
        assert self.is_started()
        self.status = TaskStatus.FAILED
        self.error = error
        self.error_stack_trace = formatted_stack_trace

    def get_filename_friendly_description(self) -> str:
        raise NotImplementedError()

    def to_plain_dictionary(self) -> Dict[str, Any]:
        return {
            "index_name": self.index_name,
            "status": self.status.value,
            "error": str(self.error) if self.error else None,
            "error_stack_trace": self.error_stack_trace
        }


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

    def __str__(self) -> str:
        start_time = datetime.datetime.utcfromtimestamp(self.start_timestamp)
        end_time = datetime.datetime.utcfromtimestamp(self.end_timestamp)

        return f"( ⚙ {self.index_name.rjust(INDEX_NAME_FORMAT_RJUST)}, {start_time} <> {end_time} )"

    def get_filename_friendly_description(self) -> str:
        return f"{self.index_name}_{self.start_timestamp}_{self.end_timestamp}"

    def to_plain_dictionary(self) -> Dict[str, Any]:
        result = super().to_plain_dictionary()
        result.update({
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp
        })

        return result


class TaskWithoutInterval(Task):
    def __init__(self, index_name: str) -> None:
        super().__init__(index_name)

    def __str__(self) -> str:
        return f"( ⚙ {self.index_name.rjust(INDEX_NAME_FORMAT_RJUST)} )"

    def get_filename_friendly_description(self) -> str:
        return f"{self.index_name}"
