import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Protocol

from multiversxetl.task import Task
from multiversxetl.transformers import (BlocksTransformer, LogsTransformer,
                                        TokensTransformer, Transformer)


class IIndexer(Protocol):
    def get_records(self, index_name: str, start_timestamp: Optional[int] = None, end_timestamp: Optional[int] = None) -> Iterable[Dict[str, Any]]: ...


class IBqClient(Protocol):
    def load_data(self, bq_dataset: str, table_name: str, schema_path: Path, data_path: Path): ...


class IFileStorage(Protocol):
    def get_extracted_path(self, task_pretty_name: str) -> Path: ...
    def get_transformed_path(self, task_pretty_name: str) -> Path: ...
    def get_load_path(self, task_pretty_name: str) -> Path: ...
    def remove_extracted_file(self, task_pretty_name: str): ...
    def remove_transformed_file(self, task_pretty_name: str): ...


class TasksRunner:
    def __init__(
            self,
            bq_client: IBqClient,
            indexer: IIndexer,
            file_storage: IFileStorage,
            schema_folder: Path
    ) -> None:
        self.bq_client = bq_client
        self.indexer = indexer
        self.file_storage = file_storage
        self.schema_folder = schema_folder
        self.transformers: Dict[str, Transformer] = {
            "blocks": BlocksTransformer(),
            "tokens": TokensTransformer(),
            "logs": LogsTransformer()
        }

    def run(self, task: Task) -> None:
        self._do_extract(task)
        self._do_transform(task)
        self._do_load(task)

        self.file_storage.remove_extracted_file(task.get_filename_friendly_description())
        self.file_storage.remove_transformed_file(task.get_filename_friendly_description())

    def _do_extract(self, task: Task) -> None:
        logging.debug(f"_do_extract: {task}")

        records = self._extract_records_from_indexer(task)
        self._write_extracted_records_to_file(task, records)

    def _extract_records_from_indexer(self, task: Task) -> Iterable[Dict[str, Any]]:
        return self.indexer.get_records(
            task.index_name,
            task.start_timestamp,
            task.end_timestamp
        )

    def _write_extracted_records_to_file(self, task: Task, records: Iterable[Dict[str, Any]]) -> None:
        filename = self.file_storage.get_extracted_path(task.get_filename_friendly_description())
        num_written = 0

        with open(filename, "w") as file:
            for record in records:
                as_json = self._jsonify_extracted_record(record)
                file.write(f"{as_json}\n")

                num_written += 1
                if num_written % 1000 == 0:
                    logging.debug(f"Written {num_written} records to {filename}")

    def _jsonify_extracted_record(self, record: Dict[str, Any]) -> str:
        data = record["_source"]
        data["_id"] = record["_id"]
        as_json = json.dumps(data)
        return as_json

    def _do_transform(self, task: Task):
        logging.debug(f"_do_transform: {task}")

        transformer = self.transformers.get(task.index_name, Transformer())
        input_filename = self.file_storage.get_extracted_path(task.get_filename_friendly_description())
        output_filename = self.file_storage.get_transformed_path(task.get_filename_friendly_description())

        with open(input_filename) as file:
            with open(output_filename, "w") as output_file:
                for line in file:
                    transformed_line = transformer.transform_json(line)
                    output_file.write(transformed_line + "\n")

    def _do_load(self, task: Task) -> None:
        logging.debug(f"_do_load: {task}")

        file_path = self.file_storage.get_load_path(task.get_filename_friendly_description())

        self.bq_client.load_data(
            bq_dataset=task.bq_dataset,
            table_name=task.index_name,
            schema_path=self.schema_folder / f"{task.index_name}.json",
            data_path=file_path
        )
