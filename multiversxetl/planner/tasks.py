
from enum import Enum
from typing import Any, Dict, List, Optional


class TaskStatus(Enum):
    PENDING = "pending"
    ASSIGNED = "assigned"
    EXTRACTED = "extracted"
    EXTRACTION_FAILED = "extraction_failed"
    LOADED = "loaded"
    LOADING_FAILED = "loading_failed"


class Task:
    def __init__(self,
                 id: str,
                 indexer_url: str,
                 index_name: str,
                 start_timestamp: Optional[int] = None,
                 end_timestamp: Optional[int] = None):
        self.id = id
        self.indexer_url = indexer_url
        self.index_name = index_name
        self.start_timestamp = start_timestamp
        self.end_timestamp = end_timestamp

        self.worker_id: str = ""
        self.status: TaskStatus = TaskStatus.PENDING
        self.progress: float = 0.0

    @classmethod
    def from_dict(cls, data: Dict[str, Any]):
        task = cls(
            id=data["id"],
            indexer_url=data["indexer_url"],
            index_name=data["index_name"],
            start_timestamp=data["start_timestamp"],
            end_timestamp=data["end_timestamp"]
        )

        task.worker_id = data["worker_id"]
        task.status = TaskStatus(data["status"])
        task.progress = data["progress"]

        return task

    def to_dict(self):
        return {
            "id": self.id,
            "indexer_url": self.indexer_url,
            "index_name": self.index_name,
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "worker_id": self.worker_id,
            "status": self.status.value,
            "progress": self.progress
        }

    def is_pending(self):
        return self.status == TaskStatus.PENDING

    def update_assign(self, worker_id: str) -> Dict[str, Any]:
        self.worker_id = worker_id
        self.status = TaskStatus.ASSIGNED

        return {
            "worker_id": self.worker_id,
            "status": self.status.value
        }

    def is_time_bound(self) -> bool:
        return self.start_timestamp is not None and self.end_timestamp is not None

    def get_extraction_filename(self) -> str:
        return f"{self.index_name}_{self.start_timestamp}_{self.end_timestamp}_{self.id}.json"


def group_tasks_by_status(tasks: List[Task]) -> Dict[TaskStatus, List[Task]]:
    tasks_by_status: Dict[TaskStatus, List[Task]] = {}

    for status in TaskStatus:
        tasks_by_status[status] = []

    for task in tasks:
        tasks_by_status[task.status].append(task)

    return tasks_by_status


def count_tasks_by_status(tasks: List[Task]) -> Dict[TaskStatus, int]:
    tasks_by_status: Dict[TaskStatus, int] = {}

    for status in TaskStatus:
        tasks_by_status[status] = 0

    for task in tasks:
        tasks_by_status[task.status] += 1

    return tasks_by_status
