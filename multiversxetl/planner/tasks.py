
from enum import Enum
from typing import Any, Dict, Optional


class TaskStatus(Enum):
    PENDING = 0
    ASSIGNED = 1
    COMPLETED = 2
    FAILED = 3


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
        return {
            "worker_id": worker_id,
            "status": TaskStatus.ASSIGNED.value
        }

    def is_same_as(self):
        pass
