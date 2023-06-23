
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple


class TaskStatus(Enum):
    """
    Allows us to track the status of a task.

    Ideally (if no errors occur), the status should go from "pending" to "finished", as follows:
        - "pending extraction" with "pending loading"
        - "started extraction" with "pending loading"
        - "finished extraction" with "pending loading"
        - "finished extraction" with "started loading"
        - "finished extraction" with "finished loading"
    """
    PENDING = "pending"
    STARTED = "started"
    FINISHED = "finished"
    FAILED = "failed"


class Task:
    def __init__(self,
                 id: str,
                 indexer_url: str,
                 index_name: str,
                 bq_dataset: str,
                 start_timestamp: Optional[int] = None,
                 end_timestamp: Optional[int] = None):
        self.id = id
        self.indexer_url = indexer_url
        self.index_name = index_name
        self.bq_dataset = bq_dataset
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

        self.extraction_status: TaskStatus = TaskStatus.PENDING
        self.extraction_worker_id: Optional[str] = None
        self.extraction_outcome: str = ""

        self.loading_status: TaskStatus = TaskStatus.PENDING
        self.loading_worker_id: Optional[str] = None
        self.loading_outcome: str = ""

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        task = cls(
            id=data["id"],
            indexer_url=data["indexer_url"],
            index_name=data["index_name"],
            bq_dataset=data["bq_dataset"],
            start_timestamp=data["start_timestamp"],
            end_timestamp=data["end_timestamp"]
        )

        task.extraction_status = TaskStatus(data["extraction_status"])
        task.extraction_worker_id = data["extraction_worker_id"]
        task.extraction_outcome = data["extraction_outcome"]

        task.loading_status = TaskStatus(data["loading_status"])
        task.loading_worker_id = data["loading_worker_id"]
        task.loading_outcome = data["loading_outcome"]

        return task

    def to_dict(self):
        return {
            "id": self.id,
            "indexer_url": self.indexer_url,
            "index_name": self.index_name,
            "bq_dataset": self.bq_dataset,
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,

            "extraction_status": self.extraction_status.value,
            "extraction_worker_id": self.extraction_worker_id,
            "extraction_outcome": self.extraction_outcome,

            "loading_status": self.loading_status.value,
            "loading_worker_id": self.loading_worker_id,
            "loading_outcome": self.loading_outcome
        }

    def is_extraction_pending(self):
        return self.extraction_status == TaskStatus.PENDING

    def is_extraction_finished(self):
        return self.extraction_status == TaskStatus.FINISHED

    def is_loading_pending(self):
        return self.loading_status == TaskStatus.PENDING

    def update_on_extraction_started(self, worker_id: str) -> Dict[str, Any]:
        self.extraction_worker_id = worker_id
        self.extraction_status = TaskStatus.STARTED

        return {
            "extraction_worker_id": self.extraction_worker_id,
            "extraction_status": self.extraction_status.value
        }

    def update_on_extraction_failure(self, outcome: str) -> Dict[str, Any]:
        self.extraction_status = TaskStatus.FAILED
        self.extraction_outcome = outcome

        return {
            "extraction_status": self.extraction_status.value,
            "extraction_outcome": self.extraction_outcome
        }

    def update_on_extraction_finished(self, outcome: str) -> Dict[str, Any]:
        self.extraction_status = TaskStatus.FINISHED
        self.extraction_outcome = outcome

        return {
            "extraction_status": self.extraction_status.value,
            "extraction_outcome": self.extraction_outcome
        }

    def update_on_loading_started(self, worker_id: str) -> Dict[str, Any]:
        self.loading_worker_id = worker_id
        self.loading_status = TaskStatus.STARTED

        return {
            "loading_worker_id": self.loading_worker_id,
            "loading_status": self.loading_status.value
        }

    def update_on_loading_failure(self, outcome: str) -> Dict[str, Any]:
        self.loading_status = TaskStatus.FAILED
        self.loading_outcome = str(outcome)

        return {
            "loading_status": self.loading_status.value,
            "loading_outcome": self.loading_outcome
        }

    def update_on_loading_finished(self, outcome: str) -> Dict[str, Any]:
        self.loading_status = TaskStatus.FINISHED
        self.loading_outcome = outcome

        return {
            "loading_status": self.loading_status.value,
            "loading_outcome": self.loading_outcome
        }

    def is_time_bound(self) -> bool:
        return self.start_timestamp is not None and self.end_timestamp is not None

    def get_pretty_name(self) -> str:
        return f"{self.index_name}_{self.start_timestamp}_{self.end_timestamp}_{self.id}"


def group_tasks_by_status(tasks: List[Task]) -> Tuple[Dict[TaskStatus, List[Task]], Dict[TaskStatus, List[Task]]]:
    tasks_by_extraction_status: Dict[TaskStatus, List[Task]] = {}
    tasks_by_loading_status: Dict[TaskStatus, List[Task]] = {}

    for status in TaskStatus:
        tasks_by_extraction_status[status] = []
        tasks_by_loading_status[status] = []

    for task in tasks:
        tasks_by_extraction_status[task.extraction_status].append(task)
        tasks_by_loading_status[task.loading_status].append(task)

    return tasks_by_extraction_status, tasks_by_loading_status


def count_tasks_by_status(tasks: List[Task]) -> Tuple[Dict[TaskStatus, int], Dict[TaskStatus, int]]:
    tasks_by_extraction_status: Dict[TaskStatus, int] = {}
    tasks_by_loading_status: Dict[TaskStatus, int] = {}

    for status in TaskStatus:
        tasks_by_extraction_status[status] = 0
        tasks_by_loading_status[status] = 0

    for task in tasks:
        tasks_by_extraction_status[task.extraction_status] += 1
        tasks_by_loading_status[task.loading_status] += 1

    return tasks_by_extraction_status, tasks_by_loading_status
