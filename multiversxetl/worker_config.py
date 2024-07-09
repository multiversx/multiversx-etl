import json
from pathlib import Path
from typing import Any, Dict, List


class WorkerConfig:
    def __init__(
            self,
            gcp_project_id: str,
            schema_folder: Path,
            indexer_url: str,
            indexer_username: str,
            indexer_password: str,
            genesis_timestamp: int,
            append_only_indices: 'IndicesConfig',
            mutable_indices: 'IndicesConfig'
    ) -> None:
        self.gcp_project_id = gcp_project_id
        self.schema_folder = schema_folder
        self.indexer_url = indexer_url
        self.indexer_username = indexer_username
        self.indexer_password = indexer_password
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
            indexer_username=data.get("indexer_username", ""),
            indexer_password=data.get("indexer_password", ""),
            genesis_timestamp=data["genesis_timestamp"],
            append_only_indices=IndicesConfig.load_from_dict(data["append_only_indices"]),
            mutable_indices=IndicesConfig.load_from_dict(data["mutable_indices"])
        )


class IndicesConfig:
    def __init__(
            self,
            bq_dataset: str,
            bq_data_transfer_name: str,
            indices: List[str],
            indices_without_timestamp: List[str],
            time_partition_start: int,
            time_partition_end: int,
            interval_size_in_seconds: int,
            num_intervals_in_bulk: int,
            num_threads: int,
            should_fail_on_counts_mismatch: bool,
            skip_counts_check_for_indices: List[str],
            counts_checks_errata: "CountChecksErrata"
    ) -> None:
        self.bq_dataset = bq_dataset
        self.bq_data_transfer_name = bq_data_transfer_name
        self.indices = indices
        self.indices_without_timestamp = indices_without_timestamp
        self.time_partition_start = time_partition_start
        self.time_partition_end = time_partition_end
        self.interval_size_in_seconds = interval_size_in_seconds
        self.num_intervals_in_bulk = num_intervals_in_bulk
        self.num_threads = num_threads
        self.should_fail_on_counts_mismatch = should_fail_on_counts_mismatch
        self.skip_counts_check_for_indices = skip_counts_check_for_indices
        self.counts_checks_errata = counts_checks_errata

    @classmethod
    def load_from_dict(cls, data: Dict[str, Any]) -> "IndicesConfig":
        return cls(
            bq_dataset=data["bq_dataset"],
            bq_data_transfer_name=data.get("bq_data_transfer_name", ""),
            indices=data["indices"],
            indices_without_timestamp=data.get("indices_without_timestamp", []),
            time_partition_start=data["time_partition_start"],
            time_partition_end=data["time_partition_end"],
            interval_size_in_seconds=data["interval_size_in_seconds"],
            num_intervals_in_bulk=data["num_intervals_in_bulk"],
            num_threads=data["num_threads"],
            should_fail_on_counts_mismatch=data["should_fail_on_counts_mismatch"],
            skip_counts_check_for_indices=data.get("skip_counts_check_for_indices", []),
            counts_checks_errata=CountChecksErrata.load_from_dict(data.get("counts_checks_errata", {}))
        )


class CountChecksErrata:
    def __init__(self, data: Dict[str, int]) -> None:
        self.data: Dict[str, int] = data

    @classmethod
    def load_from_dict(cls, data: Dict[str, Any]) -> "CountChecksErrata":
        return cls(
            data=data
        )

    def get_erratum(self, table: str) -> int:
        return self.data.get(table, 0)
