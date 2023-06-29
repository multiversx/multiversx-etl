
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional

from multiversxetl.jobs.extract_job import ExtractJob
from multiversxetl.planner.tasks import Task


class IndexerMock:
    def __init__(self) -> None:
        self.mock_records_with_interval: List[Dict[str, Any]] = []
        self.mock_records_without_interval: List[Dict[str, Any]] = []

        self.recorded_index_name: str = ""
        self.recorded_start_timestamp: Optional[int] = None
        self.recorded_end_timestamp: Optional[int] = None

    def get_records_with_interval(self, index_name: str, start_timestamp: int, end_timestamp: int) -> Iterable[Dict[str, Any]]:
        self.recorded_index_name = index_name
        self.recorded_start_timestamp = start_timestamp
        self.recorded_end_timestamp = end_timestamp

        return self.mock_records_with_interval

    def get_records_without_interval(self, index_name: str) -> Iterable[Dict[str, Any]]:
        self.recorded_index_name = index_name
        self.recorded_start_timestamp = None
        self.recorded_end_timestamp = None

        return self.mock_records_without_interval


class FileStorageMock:
    def get_extracted_path(self, task_pretty_name: str) -> Path:
        return Path(__file__).parent.parent / "testdata" / f"extracted_{task_pretty_name}.json"


def test_run_task_with_interval():
    indexer = IndexerMock()
    file_storage = FileStorageMock()

    task = Task("test", "test", "test", "test", 0, 1)
    output_file = file_storage.get_extracted_path(task.get_pretty_name())
    job = ExtractJob(indexer, file_storage, task)

    indexer.mock_records_with_interval = [
        {"_id": "1", "_source": {"a": "1"}},
        {"_id": "2", "_source": {"b": "2"}},
        {"_id": "3", "_source": {"c": "3"}},
    ]

    job.run()
    assert indexer.recorded_index_name == "test"
    assert indexer.recorded_start_timestamp == 0
    assert indexer.recorded_end_timestamp == 1

    lines = output_file.read_text().splitlines()
    assert len(lines) == 3
    assert lines[0] == '{"a": "1", "_id": "1"}'
    assert lines[1] == '{"b": "2", "_id": "2"}'
    assert lines[2] == '{"c": "3", "_id": "3"}'

    output_file.unlink(missing_ok=True)


def test_run_task_without_interval():
    indexer = IndexerMock()
    file_storage = FileStorageMock()

    task = Task("test", "test", "test", "test")
    output_file = file_storage.get_extracted_path(task.get_pretty_name())
    job = ExtractJob(indexer, file_storage, task)

    indexer.mock_records_without_interval = [
        {"_id": "1", "_source": {"a": "1"}},
        {"_id": "2", "_source": {"b": "2"}},
        {"_id": "3", "_source": {"c": "3"}},
    ]

    job.run()
    assert indexer.recorded_index_name == "test"
    assert indexer.recorded_start_timestamp is None
    assert indexer.recorded_end_timestamp is None

    lines = output_file.read_text().splitlines()
    assert len(lines) == 3
    assert lines[0] == '{"a": "1", "_id": "1"}'
    assert lines[1] == '{"b": "2", "_id": "2"}'
    assert lines[2] == '{"c": "3", "_id": "3"}'

    output_file.unlink(missing_ok=True)
