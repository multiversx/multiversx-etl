import logging
from pathlib import Path
from typing import Any, List, Optional

import requests
from google.cloud import bigquery

WRITE_DISPOSITION_APPEND = "WRITE_APPEND"


class BqClient:
    def __init__(self, gcp_project_id: str) -> None:
        client = bigquery.Client(project=gcp_project_id)
        adapter = requests.adapters.HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=3)  # type: ignore
        client._http.mount("https://", adapter)  # type: ignore
        client._http._auth_request.session.mount("https://", adapter)  # type: ignore

        self.client = client

    def truncate_tables(self, bq_dataset: str, tables: List[str]) -> None:
        for table in tables:
            table_ref = self.client.dataset(bq_dataset).table(table)
            self.client.delete_table(table_ref)

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
