import json
import logging
from pathlib import Path
from typing import Any, Dict, Iterable, Protocol

from google.cloud import bigquery

from multiversxetl.task import Task, TaskWithInterval, TaskWithoutInterval
from multiversxetl.transformers import (BlocksTransformer, LogsTransformer,
                                        TokensTransformer, Transformer)

WRITE_DISPOSITION_APPEND = "WRITE_APPEND"
WRITE_DISPOSITION_TRUNCATE = "WRITE_TRUNCATE"


class IIndexer(Protocol):
    def get_records_with_interval(self, index_name: str, start_timestamp: int, end_timestamp: int) -> Iterable[Dict[str, Any]]: ...
    def get_records_without_interval(self, index_name: str) -> Iterable[Dict[str, Any]]: ...


class IFileStorage(Protocol):
    def get_extracted_path(self, task_pretty_name: str) -> Path: ...
    def get_transformed_path(self, task_pretty_name: str) -> Path: ...
    def get_load_path(self, task_pretty_name: str) -> Path: ...
    def remove_extracted_file(self, task_pretty_name: str): ...
    def remove_transformed_file(self, task_pretty_name: str): ...


class TasksRunner:
    def __init__(
            self,
            bq_client: bigquery.Client,
            bq_dataset: str,
            indexer: IIndexer,
            file_storage: IFileStorage,
            schema_folder: Path
    ) -> None:
        self.bq_client = bq_client
        self.bq_dataset = bq_dataset
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
        if isinstance(task, TaskWithInterval):
            assert task.start_timestamp is not None
            assert task.end_timestamp is not None

            return self.indexer.get_records_with_interval(
                task.index_name,
                task.start_timestamp,
                task.end_timestamp
            )
        elif isinstance(task, TaskWithoutInterval):
            return self.indexer.get_records_without_interval(task.index_name)

        raise NotImplementedError()

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

        table_id = self._get_bq_table_id(task)
        file_path = self.file_storage.get_load_path(task.get_filename_friendly_description())
        job_config = self._prepare_bq_job_config(task)

        with open(file_path, "rb") as source_file:
            job = self.bq_client.load_table_from_file(source_file, table_id, job_config=job_config)

        # Waits for the job to complete.
        job.result()

        table: Any = self.bq_client.get_table(table_id)
        logging.debug(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    def _get_bq_table_id(self, task: Task) -> str:
        return f"{self.bq_dataset}.{task.index_name}"

    def _prepare_bq_job_config(self, task: Task) -> bigquery.LoadJobConfig:
        schema_path = self.schema_folder / f"{task.index_name}.json"
        schema = self.bq_client.schema_from_json(schema_path)
        write_disposition = self._get_bq_write_disposition(task)

        return bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition
        )

    def _get_bq_write_disposition(self, task: Task) -> str:
        if isinstance(task, TaskWithInterval):
            return WRITE_DISPOSITION_APPEND
        return WRITE_DISPOSITION_TRUNCATE
