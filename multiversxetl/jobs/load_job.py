from typing import Any, Optional, Protocol

from google.cloud import bigquery


class ITask(Protocol):
    @property
    def indexer_url(self) -> str: ...
    @property
    def index_name(self) -> str: ...
    @property
    def bq_dataset(self) -> str: ...
    def is_time_bound(self) -> bool: ...
    @property
    def start_timestamp(self) -> Optional[int]: ...
    @property
    def end_timestamp(self) -> Optional[int]: ...
    def get_transformed_filename(self) -> str: ...


class LoadJob:
    def __init__(self, gcp_project_id: str, task: ITask) -> None:
        self.gcp_project_id = gcp_project_id
        self.task = task
        self.bigquery_client = bigquery.Client()

    def run(self) -> None:
        table_id = self._get_table_id()
        write_disposition = self._get_write_disposition()
        file_path = self._get_file_path()

        job_config = bigquery.LoadJobConfig(
            autodetect=True,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=write_disposition
        )

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
        return f"{self.gcp_project_id}.{self.task.bq_dataset}.{self.task.index_name}"

    def _get_write_disposition(self) -> str:
        if self.task.is_time_bound():
            return "WRITE_APPEND"
        return "WRITE_TRUNCATE"

    def _get_file_path(self) -> str:
        return self.task.get_transformed_filename()
