# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import Any

from google.cloud import bigquery

from multiversxetl.jobs.load_job import (WRITE_DISPOSITION_APPEND,
                                         WRITE_DISPOSITION_TRUNCATE, LoadJob)
from multiversxetl.planner.tasks import Task

schema_folder = Path(__file__).parent.parent.parent / "schema"


class FileStorageMock:
    def get_load_path(self, task_pretty_name: str) -> Path:
        return Path(__file__).parent / f"transformed_{task_pretty_name}.json"


def test_get_table_id():
    file_storage = FileStorageMock()
    task = Task("test", "", "test_index_name", "test_dataset", 0, 1)
    job = LoadJob("test-project", file_storage, task, schema_folder)

    assert job._get_table_id() == "test_dataset.test_index_name"


def test_prepare_job_config():
    file_storage = FileStorageMock()

    task = Task("test", "", "transactions", "test_dataset", 0, 1)
    job = LoadJob("test-project", file_storage, task, schema_folder)
    job_config: Any = job._prepare_job_config()

    assert job_config.write_disposition == WRITE_DISPOSITION_APPEND
    assert job_config.source_format == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    assert len(job_config.schema) == 38

    task = Task("test", "", "accounts", "test_dataset")
    job = LoadJob("test-project", file_storage, task, schema_folder)
    job_config: Any = job._prepare_job_config()

    assert job_config.write_disposition == WRITE_DISPOSITION_TRUNCATE
    assert job_config.source_format == bigquery.SourceFormat.NEWLINE_DELIMITED_JSON
    assert len(job_config.schema) == 13
