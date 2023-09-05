import json
from pathlib import Path
from typing import Any, Dict, List


class WorkerConfig:
    def __init__(
            self,
            gcp_project_id: str,
            bq_dataset: str,
            schema_folder: Path,
            indexer_url: str,
            indices_with_intervals: List[str],
            indices_without_intervals: List[str],
            time_partition_start: int,
            time_partition_end: int,
            interval_size_in_seconds: int,
            num_intervals_in_bulk: int,
            num_threads: int
    ) -> None:
        self.gcp_project_id = gcp_project_id
        self.bq_dataset = bq_dataset
        self.schema_folder = schema_folder
        self.indexer_url = indexer_url
        self.indices_with_intervals = indices_with_intervals
        self.indices_without_intervals = indices_without_intervals
        self.time_partition_start = time_partition_start
        self.time_partition_end = time_partition_end
        self.interval_size_in_seconds = interval_size_in_seconds
        self.num_intervals_in_bulk = num_intervals_in_bulk
        self.num_threads = num_threads

    @classmethod
    def load_from_file(cls, path: Path) -> "WorkerConfig":
        data = json.loads(path.read_text())
        return cls.load_from_dict(data)

    @classmethod
    def load_from_dict(cls, data: Dict[str, Any]) -> "WorkerConfig":
        return cls(
            gcp_project_id=data["gcp_project_id"],
            bq_dataset=data["bq_dataset"],
            schema_folder=Path(data["schema_folder"]),
            indexer_url=data["indexer_url"],
            indices_with_intervals=data["indices_with_intervals"],
            indices_without_intervals=data["indices_without_intervals"],
            time_partition_start=data["time_partition_start"],
            time_partition_end=data["time_partition_end"],
            interval_size_in_seconds=data["interval_size_in_seconds"],
            num_intervals_in_bulk=data["num_intervals_in_bulk"],
            num_threads=data["num_threads"]
        )
