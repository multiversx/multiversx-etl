import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Protocol


class IIndexer(Protocol):
    def get_records_with_interval(self, index_name: str, start_timestamp: int, end_timestamp: int) -> Iterable[Dict[str, Any]]: ...
    def get_records_without_interval(self, index_name: str) -> Iterable[Dict[str, Any]]: ...


class IFileStorage(Protocol):
    def get_extracted_path(self, task_pretty_name: str) -> Path: ...


class ITask(Protocol):
    @property
    def index_name(self) -> str: ...
    def is_time_bound(self) -> bool: ...
    @property
    def start_timestamp(self) -> Optional[int]: ...
    @property
    def end_timestamp(self) -> Optional[int]: ...
    def get_pretty_name(self) -> str: ...


class ExtractJob:
    def __init__(self,
                 indexer: IIndexer,
                 file_storage: IFileStorage,
                 task: ITask) -> None:
        self.indexer = indexer
        self.file_storage = file_storage
        self.task = task

    def run(self) -> None:
        records = self._fetch_records()
        self._write_records_to_file(records)

    def _fetch_records(self) -> Iterable[Dict[str, Any]]:
        if self.task.is_time_bound():
            assert self.task.start_timestamp is not None
            assert self.task.end_timestamp is not None

            return self.indexer.get_records_with_interval(
                self.task.index_name,
                self.task.start_timestamp,
                self.task.end_timestamp
            )

        return self.indexer.get_records_without_interval(self.task.index_name)

    def _write_records_to_file(self, records: Iterable[Dict[str, Any]]) -> None:
        filename = self.file_storage.get_extracted_path(self.task.get_pretty_name())
        num_written = 0

        with open(filename, "w") as file:
            for record in records:
                as_json = self._jsonify_record(record)
                file.write(f"{as_json}\n")

                num_written += 1
                if num_written % 1000 == 0:
                    print(f"Written {num_written} records to {filename}")

    def _jsonify_record(self, record: Dict[str, Any]) -> str:
        data = record["_source"]
        data["_id"] = record["_id"]
        as_json = json.dumps(data)
        return as_json
