import datetime
import json
from pathlib import Path
from typing import Any, Dict


class WorkerState:
    def __init__(
        self,
        latest_checkpoint_timestamp: int
    ) -> None:
        self.latest_checkpoint_timestamp = latest_checkpoint_timestamp

    def get_latest_checkpoint_datetime(self) -> datetime.datetime:
        return datetime.datetime.fromtimestamp(self.latest_checkpoint_timestamp, tz=datetime.timezone.utc)

    @classmethod
    def load_from_file(cls, path: Path) -> "WorkerState":
        data_json = path.read_text()
        data = json.loads(data_json)
        return cls.load_from_dict(data)

    @classmethod
    def load_from_dict(cls, data: Dict[str, Any]) -> "WorkerState":
        return cls(
            latest_checkpoint_timestamp=data.get("latest_checkpoint_timestamp", 0)
        )

    def save_to_file(self, path: Path) -> None:
        data = self.to_plain_dictionary()
        data_json = json.dumps(data, indent=4)
        path.write_text(data_json)

    def to_plain_dictionary(self) -> Dict[str, Any]:
        return {
            "latest_checkpoint_timestamp": self.latest_checkpoint_timestamp
        }
