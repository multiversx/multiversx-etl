import json
from pathlib import Path
from typing import Any, Dict, List


class WorkerConfig:
    def __init__(
            self,
            gcp_project_id: str,
            schema_folder: Path,
            indexer_url: str,
            genesis_timestamp: int,
            append_only_indices: 'IndicesConfig',
            mutable_indices: 'IndicesConfig'
    ) -> None:
        self.gcp_project_id = gcp_project_id
        self.schema_folder = schema_folder
        self.indexer_url = indexer_url
        self.genesis_timestamp = genesis_timestamp
        self.append_only_indices = append_only_indices
        self.mutable_indices = mutable_indices

    @classmethod
    def load_from_file(cls, path: Path) -> "WorkerConfig":
        data = json.loads(path.read_text())
        return cls.load_from_dict(data)

    @classmethod
    def load_from_dict(cls, data: Dict[str, Any]) -> "WorkerConfig":
        return cls(
            gcp_project_id=data["gcp_project_id"],
            schema_folder=Path(data["schema_folder"]),
            indexer_url=data["indexer_url"],
            genesis_timestamp=data["genesis_timestamp"],
            append_only_indices=IndicesConfig.load_from_dict(data["append_only_indices"]),
            mutable_indices=IndicesConfig.load_from_dict(data["mutable_indices"])
        )


class IndicesConfig:
    def __init__(
            self,
            bq_dataset: str,
            indices: List[str],
            time_partition_start: int,
            time_partition_end: int,
            interval_size_in_seconds: int,
            num_intervals_in_bulk: int,
            num_threads: int,
            should_fail_on_counts_mismatch: bool,
    ) -> None:
        self.bq_dataset = bq_dataset
        self.indices = indices
        self.time_partition_start = time_partition_start
        self.time_partition_end = time_partition_end
        self.interval_size_in_seconds = interval_size_in_seconds
        self.num_intervals_in_bulk = num_intervals_in_bulk
        self.num_threads = num_threads
        self.should_fail_on_counts_mismatch = should_fail_on_counts_mismatch

    @classmethod
    def load_from_dict(cls, data: Dict[str, Any]) -> "IndicesConfig":
        return cls(
            bq_dataset=data["bq_dataset"],
            indices=data["indices"],
            time_partition_start=data["time_partition_start"],
            time_partition_end=data["time_partition_end"],
            interval_size_in_seconds=data["interval_size_in_seconds"],
            num_intervals_in_bulk=data["num_intervals_in_bulk"],
            num_threads=data["num_threads"],
            should_fail_on_counts_mismatch=data["should_fail_on_counts_mismatch"]
        )
