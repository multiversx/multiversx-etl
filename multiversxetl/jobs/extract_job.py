import json
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Protocol

import elasticsearch.helpers
from elasticsearch import Elasticsearch

SCROLL_CONSISTENCY_TIME = "10m"
SCAN_BATCH_SIZE = 1000


class IFileStorage(Protocol):
    def get_extracted_path(self, task_pretty_name: str) -> Path: ...


class ITask(Protocol):
    @property
    def indexer_url(self) -> str: ...
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
                 file_storage: IFileStorage,
                 task: ITask) -> None:
        self.file_storage = file_storage
        self.task = task
        self.elastic_search_client = Elasticsearch(self.task.indexer_url)

    def run(self) -> None:
        query = self._create_query()

        records = elasticsearch.helpers.scan(
            client=self.elastic_search_client,
            index=self.task.index_name,
            query=query,
            scroll=SCROLL_CONSISTENCY_TIME,
            raise_on_error=True,
            preserve_order=False,
            size=SCAN_BATCH_SIZE,
            request_timeout=None,
            scroll_kwargs=None
        )

        self._write_records_to_file(records)

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

    def _create_query(self) -> Any:
        if self.task.is_time_bound():
            return {
                "query": {
                    "range": {
                        "timestamp": {
                            "gte": self.task.start_timestamp,
                            "lte": self.task.end_timestamp,
                        },
                    }
                }
            }

        return {
            "query": {
                "match_all": {},
            }
        }
