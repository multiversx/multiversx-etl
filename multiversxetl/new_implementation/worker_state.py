import json
from pathlib import Path
from typing import Any, Dict


class WorkerState:
    def __init__(
        self,
        latest_finished_interval_end_time: int,
    ) -> None:
        self.latest_finished_interval_end_time = latest_finished_interval_end_time

    @classmethod
    def load_from_file(cls, path: Path) -> "WorkerState":
        data = json.loads(path.read_text())
        return cls.load_from_dict(data)

    @classmethod
    def load_from_dict(cls, data: Dict[str, Any]) -> "WorkerState":
        return cls(
            latest_finished_interval_end_time=data.get("latest_finished_interval_end_time", 0)
        )
