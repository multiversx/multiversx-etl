# pyright: reportPrivateUsage=false

import concurrent.futures
import os
from typing import Any, List

import pytest

from multiversxetl.errors import TransientError
from multiversxetl.planner.tasks import Task
from multiversxetl.planner.tasks_storage import (TasksStorage,
                                                 _is_transient_error,
                                                 _split_to_chunks)


def test_is_transient_error():
    assert _is_transient_error(Exception("Please try again"))
    assert _is_transient_error(Exception("Aborted due to cross-transaction contention"))
    assert _is_transient_error(Exception("Failed to commit transaction"))
    assert not _is_transient_error(Exception("Some error"))


def test_split_to_chunks():
    assert _split_to_chunks([1, 2, 3, 4, 5], 2) == [[1, 2], [3, 4], [5]]
    assert _split_to_chunks([1, 2, 3, 4, 5], 3) == [[1, 2, 3], [4, 5]]
    assert _split_to_chunks([1, 2, 3, 4, 5], 5) == [[1, 2, 3, 4, 5]]


@pytest.mark.integration
def test_add_tasks_then_do_concurrent_take_any_extract_task():
    project_id: str = os.environ.get("GCP_PROJECT_ID", "")
    assert project_id, "GCP_PROJECT_ID env var is not set"
    collection_name = test_add_tasks_then_do_concurrent_take_any_extract_task.__name__

    num_tasks = 64
    num_workers = 4
    num_tasks_done = 0

    storage = TasksStorage(project_id, collection_name)

    dummy_tasks: List[Task] = [Task(str(i), "", "", "", 0, 0) for i in range(0, num_tasks)]
    storage.add_tasks(dummy_tasks)

    while num_tasks_done != num_tasks:
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_workers) as executor:
            future_take: Any = {executor.submit(storage.take_any_extract_task, "worker", None): _ for _ in range(num_tasks)}

            for future in concurrent.futures.as_completed(future_take):
                try:
                    task = future.result()

                    if task is None:
                        # No more tasks to take
                        pass
                    else:
                        storage.update_task(task.id, lambda t: t.update_on_extraction_finished(""))
                        num_tasks_done += 1
                except TransientError:
                    # Transient errors are acceptable
                    pass

    # Cleanup
    storage.delete_all_tasks()
