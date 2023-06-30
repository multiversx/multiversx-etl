from pathlib import Path
from typing import Any, Optional, Protocol

from google.cloud import bigquery

WRITE_DISPOSITION_APPEND = "WRITE_APPEND"
WRITE_DISPOSITION_TRUNCATE = "WRITE_TRUNCATE"


class IFileStorage(Protocol):
    def get_load_path(self, task_pretty_name: str) -> Path: ...


class ITask(Protocol):
    @property
    def index_name(self) -> str: ...
    @property
    def bq_dataset(self) -> str: ...
    def is_time_bound(self) -> bool: ...
    @property
    def start_timestamp(self) -> Optional[int]: ...
    @property
    def end_timestamp(self) -> Optional[int]: ...
    def get_pretty_name(self) -> str: ...


class LoadJob:
    def __init__(self,
                 gcp_project_id: str,
                 file_storage: IFileStorage,
                 task: ITask,
                 schema_folder: Path) -> None:
        self.file_storage = file_storage
        self.task = task
        self.schema_folder = schema_folder
        self.bigquery_client = bigquery.Client(project=gcp_project_id)

    def run(self) -> None:
        table_id = self._get_table_id()
        file_path = self.file_storage.get_load_path(self.task.get_pretty_name())
        job_config = self._prepare_job_config()

        with open(file_path, "rb") as source_file:
            job = self.bigquery_client.load_table_from_file(source_file, table_id, job_config=job_config)

        # Waits for the job to complete.
        job.result()

        table: Any = self.bigquery_client.get_table(table_id)
        print(
            "Loaded {} rows and {} columns to {}".format(
                table.num_rows, len(table.schema), table_id
            )
        )

    def _get_table_id(self) -> str:
        return f"{self.task.bq_dataset}.{self.task.index_name}"

    def _prepare_job_config(self) -> bigquery.LoadJobConfig:
        schema_path = self.schema_folder / f"{self.task.index_name}.json"
        schema = self.bigquery_client.schema_from_json(schema_path)
        write_disposition = self._get_write_disposition()

        return bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition
        )

    def _get_write_disposition(self) -> str:
        if self.task.is_time_bound():
            return WRITE_DISPOSITION_APPEND
        return WRITE_DISPOSITION_TRUNCATE
