import datetime
import json
from pathlib import Path
from typing import Any, Dict


class WorkerState:
    def __init__(
        self,
        latest_finished_interval_end_time: int,
    ) -> None:
        self.latest_finished_interval_end_time = latest_finished_interval_end_time

    def get_latest_finished_interval_end_datetime(self) -> datetime.datetime:
        return datetime.datetime.utcfromtimestamp(self.latest_finished_interval_end_time)

    @classmethod
    def load_from_file(cls, path: Path) -> "WorkerState":
        data_json = path.read_text()
        data = json.loads(data_json)
        return cls.load_from_dict(data)

    @classmethod
    def load_from_dict(cls, data: Dict[str, Any]) -> "WorkerState":
        return cls(
            latest_finished_interval_end_time=data.get("latest_finished_interval_end_time", 0)
        )

    def save_to_file(self, path: Path) -> None:
        data = self.to_plain_dictionary()
        data_json = json.dumps(data, indent=4)
        path.write_text(data_json)

    def to_plain_dictionary(self) -> Dict[str, Any]:
        return {
            "latest_finished_interval_end_time": self.latest_finished_interval_end_time,
        }
