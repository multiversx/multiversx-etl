import datetime
import logging
import time
from pathlib import Path
from threading import Lock
from typing import Any, List, Optional

import requests
from google.cloud import bigquery
from google.cloud.bigquery_datatransfer_v1 import (
    DataTransferServiceClient, StartManualTransferRunsRequest)

WRITE_DISPOSITION_APPEND = "WRITE_APPEND"


class BqClient:
    def __init__(self, gcp_project_id: str) -> None:
        self.gcp_project_id = gcp_project_id
        client = bigquery.Client(project=gcp_project_id)
        adapter = requests.adapters.HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=3)  # type: ignore
        client._http.mount("https://", adapter)  # type: ignore
        client._http._auth_request.session.mount("https://", adapter)  # type: ignore

        self.client = client
        self.throttler = OneEachSecondsThrottler(num_seconds=3)

    def truncate_tables(self, bq_dataset: str, tables: List[str]) -> None:
        for table in tables:
            logging.info(f"Truncating {bq_dataset}.{table}...")

            table_ref = self.client.dataset(bq_dataset).table(table)
            self.client.delete_table(table_ref, not_found_ok=True)

    def run_query(
        self,
        query_parameters: List[bigquery.ScalarQueryParameter],
        query: str,
        into_table: Optional[str] = None
    ) -> List[Any]:
        job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)

        if into_table:
            job_config.destination = f"{self.client.project}.{into_table}"
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE

        job = self.client.query(query, job_config=job_config)
        records = list(job.result())
        return records

    def load_data(
            self,
            bq_dataset: str,
            table_name: str,
            schema_path: Path,
            data_path: Path,
    ):
        self.throttler.wait_if_necessary()

        table_id = f"{bq_dataset}.{table_name}"
        logging.debug(f"Loading data into {table_id}...")

        schema = self.client.schema_from_json(schema_path)

        job_config = bigquery.LoadJobConfig(
            schema=schema,
            source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
            write_disposition=WRITE_DISPOSITION_APPEND
        )

        with open(data_path, "rb") as data_file:
            job = self.client.load_table_from_file(data_file, table_id, job_config=job_config)

        # Waits for the job to complete.
        job.result()

        table: Any = self.client.get_table(table_id)
        logging.debug(f"Loaded {table.num_rows} rows and {len(table.schema)} columns to {table_id}")

    def trigger_data_transfer(self, transfer_config_name: str):
        # https://cloud.google.com/bigquery/docs/working-with-transfers
        client = DataTransferServiceClient()
        now = datetime.datetime.now(datetime.timezone.utc)

        request = StartManualTransferRunsRequest(
            parent=transfer_config_name,
            requested_run_time=now,
        )

        response = client.start_manual_transfer_runs(request=request)  # type: ignore

        for run in response.runs:
            logging.info(f"Started manual transfer: time = {run.run_time}, name = {run.name}")


class OneEachSecondsThrottler:
    def __init__(self, num_seconds: int) -> None:
        self.mutex = Lock()
        self.latest_operation_timestamp = 0
        self.num_seconds = num_seconds

    def wait_if_necessary(self) -> None:
        while True:
            should_wait = False

            with self.mutex:
                now = int(datetime.datetime.now(tz=datetime.timezone.utc).timestamp())
                delta = now - self.latest_operation_timestamp

                if delta < self.num_seconds:
                    should_wait = True
                else:
                    self.latest_operation_timestamp = now
                    break

            if should_wait:
                time.sleep(1)