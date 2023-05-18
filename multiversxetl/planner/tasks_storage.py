from typing import Any, List

from google.cloud import firestore
from google.cloud.firestore import transactional  # type: ignore

from multiversxetl.planner.tasks import Task


class TasksStorage:
    def __init__(self, project_id: str):
        self.db = firestore.Client(project=project_id)

    def get_all_tasks(self) -> List[Task]:
        snapshots = self.db.collection("tasks").stream()
        tasks: List[Task] = []

        for snapshot in snapshots:
            snapshot_dict = snapshot.to_dict()
            assert snapshot_dict is not None
            task = Task.from_dict(snapshot_dict)
            tasks.append(task)

        return tasks

    def add_tasks(self, tasks: List[Task]):
        for tasks_chunk in split_to_chunks(tasks, 100):
            batch: Any = self.db.batch()  # type: ignore

            for task in tasks_chunk:
                task_ref = self.db.collection("tasks").document(task.id)
                batch.set(task_ref, task.to_dict())

            batch.commit()

    def assign_task_to_worker(self, task_id: str, worker_id: str):
        transaction: Any = self.db.transaction()  # type: ignore
        task_ref = self.db.collection("tasks").document(task_id)
        do_assign_task_to_worker(transaction, task_ref, worker_id)


@transactional
def do_assign_task_to_worker(transaction: Any, task_ref: Any, worker_id: str) -> None:
    snapshot = task_ref.get(transaction=transaction)
    ensure_document_exists(task_ref, snapshot)

    task = Task.from_dict(snapshot)
    if task.is_pending():
        pass

    transaction.update(task_ref, task.update_assign(worker_id))


def ensure_document_exists(document_ref: Any, snapshot: Any) -> None:
    assert snapshot.exists, f"Document {document_ref.path} does not exist"


def split_to_chunks(items: List[Any], chunk_size: int) -> List[List[Any]]:
    chunks: List[List[Any]] = []

    for i in range(0, len(items), chunk_size):
        chunk = items[i: i + chunk_size]
        chunks.append(chunk)

    return chunks
