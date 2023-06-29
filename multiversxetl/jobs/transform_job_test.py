# pyright: reportPrivateUsage=false

from pathlib import Path
from typing import Any, Dict

from multiversxetl.jobs.transform_job import Transformer, TransformJob
from multiversxetl.planner.tasks import Task


class FileStorageMock:
    def get_extracted_path(self, task_pretty_name: str) -> Path:
        return Path(__file__).parent.parent / "testdata" / f"extracted_{task_pretty_name}.txt"

    def get_transformed_path(self, task_pretty_name: str) -> Path:
        return Path(__file__).parent.parent / "testdata" / f"transformed_{task_pretty_name}.txt"


class DummyTransformer(Transformer):
    def _do_transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        del data["a"]
        return data


def test_run_with_noop_transform():
    file_storage = FileStorageMock()
    task = Task("test", "test", "foobar", "test", 0, 1)

    input_file = file_storage.get_extracted_path(task.get_pretty_name())
    output_file = file_storage.get_transformed_path(task.get_pretty_name())

    input_file.write_text("""\
{"a": 1, "b": 2, "c": 3}
{"a": 2, "b": 3, "c": 4}
{"a": 3, "b": 4, "c": 5}
""")

    job = TransformJob(file_storage, task)
    job.run()

    assert output_file.read_text() == input_file.read_text()

    input_file.unlink(missing_ok=True)
    output_file.unlink(missing_ok=True)


def test_run_with_proper_transform():
    file_storage = FileStorageMock()
    task = Task("test", "test", "foobar", "test", 0, 1)

    input_file = file_storage.get_extracted_path(task.get_pretty_name())
    output_file = file_storage.get_transformed_path(task.get_pretty_name())

    input_file.write_text("""\
{"a": 1, "b": 2, "c": 3}
{"a": 2, "b": 3, "c": 4}
{"a": 3, "b": 4, "c": 5}
""")

    job = TransformJob(file_storage, task)
    job._register_transformer("foobar", DummyTransformer())

    job.run()

    assert output_file.read_text() == """\
{"b": 2, "c": 3}
{"b": 3, "c": 4}
{"b": 4, "c": 5}
"""

    input_file.unlink(missing_ok=True)
    output_file.unlink(missing_ok=True)
