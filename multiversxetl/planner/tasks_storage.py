from typing import Any, Callable, List, Optional

from google.cloud import firestore
from google.cloud.firestore import transactional  # type: ignore
from google.cloud.firestore import CollectionReference

from multiversxetl.errors import TransientError
from multiversxetl.planner.tasks import Task, TaskStatus

INSERT_CHUNK_SIZE = 100
DELETE_CHUNK_SIZE = 100


class TasksStorage:
    def __init__(self, project_id: str, collection: str):
        self.db = firestore.Client(project=project_id)
        self.collection = collection

    def get_all_tasks(self) -> List[Task]:
        snapshots = self.db.collection(self.collection).stream()
        tasks: List[Task] = []

        for snapshot in snapshots:
            snapshot_dict = snapshot.to_dict()
            assert snapshot_dict is not None
            task = Task.from_dict(snapshot_dict)
            tasks.append(task)

        return tasks

    def delete_all_tasks(self) -> None:
        """
        See https://firebase.google.com/docs/firestore/manage-data/delete-data#collections.
        """
        docs = self.db.collection(self.collection).list_documents(page_size=DELETE_CHUNK_SIZE)
        num_deleted = 0

        for doc in docs:
            doc.delete()
            num_deleted = num_deleted + 1

        if num_deleted >= DELETE_CHUNK_SIZE:
            return self.delete_all_tasks()

    def add_tasks(self, tasks: List[Task]):
        for tasks_chunk in _split_to_chunks(tasks, INSERT_CHUNK_SIZE):
            batch: Any = self.db.batch()  # type: ignore

            for task in tasks_chunk:
                task_ref = self.db.collection(self.collection).document(task.id)
                batch.set(task_ref, task.to_dict())

            batch.commit()

    def take_any_extract_task(self,
                              worker_id: str,
                              index_name: Optional[str]) -> Optional[Task]:
        transaction: Any = self.db.transaction()  # type: ignore
        collection = self.db.collection(self.collection)
        task = _transactional_pessimistic_take_any_extraction_task(transaction, collection, worker_id, index_name)
        return task

    def take_any_load_task(self,
                           worker_id: str,
                           index_name: Optional[str]) -> Optional[Task]:
        transaction: Any = self.db.transaction()  # type: ignore
        collection = self.db.collection(self.collection)
        task = _transactional_pessimistic_take_any_loading_task(transaction, collection, worker_id, index_name)
        return task

    def update_task(self, task_id: str, update_func: Callable[[Task], Any]) -> Task:
        # We do not use a transaction, since, generally speaking,
        # we do not expect concurrent updates of the same task (once assigned to a worker).
        task_ref: Any = self.db.collection(self.collection).document(task_id)
        snapshot = task_ref.get()
        data = snapshot.to_dict()
        assert data is not None

        task = Task.from_dict(data)
        partial_update = update_func(task)
        task_ref.update(partial_update)
        return task


class TasksWithIntervalStorage(TasksStorage):
    def __init__(self, project_id: str):
        super().__init__(project_id, "tasks_with_interval")


class TasksWithoutIntervalStorage(TasksStorage):
    def __init__(self, project_id: str):
        super().__init__(project_id, "tasks_without_interval")


def _transactional_pessimistic_take_any_extraction_task(
        transaction: Any,
        collection: CollectionReference,
        worker_id: str,
        index_name: Optional[str]
) -> Optional[Task]:
    try:
        return _transactional_take_any_extraction_task(transaction, collection, worker_id, index_name)
    except Exception as error:
        if _is_transient_error(error):
            raise TransientError(str(error)) from error
        raise


@transactional
def _transactional_take_any_extraction_task(
        transaction: Any,
        collection: CollectionReference,
        worker_id: str,
        index_name: Optional[str]
) -> Optional[Task]:
    extraction_is_pending_or_failed = ("extraction_status", "in", [TaskStatus.PENDING.value, TaskStatus.FAILED.value])
    index_name_is = ("index_name", "==", index_name)

    query = collection.where(*extraction_is_pending_or_failed)  # type: ignore
    if index_name:
        query = query.where(*index_name_is)  # type: ignore

    pending_tasks = query.limit(1).stream(transaction=transaction)  # type: ignore
    snapshot = next(pending_tasks, None)

    if not snapshot:
        return None

    data = snapshot.to_dict()
    assert data is not None

    task = Task.from_dict(data)
    transaction.update(snapshot.reference, task.update_on_extraction_started(worker_id))
    return task


def _transactional_pessimistic_take_any_loading_task(
        transaction: Any,
        collection: CollectionReference,
        worker_id: str,
        index_name: Optional[str]
) -> Optional[Task]:
    try:
        return _transactional_take_any_loading_task(transaction, collection, worker_id, index_name)
    except Exception as error:
        if _is_transient_error(error):
            raise TransientError(str(error)) from error
        raise


@transactional
def _transactional_take_any_loading_task(
    transaction: Any,
    collection: CollectionReference,
    worker_id: str,
    index_name: Optional[str]
) -> Optional[Task]:
    extraction_is_finished = ("extraction_status", "==", TaskStatus.FINISHED.value)
    loading_is_pending_or_failed = ("loading_status", "in", [TaskStatus.PENDING.value, TaskStatus.FAILED.value])
    index_name_is = ("index_name", "==", index_name)

    query = collection.where(*extraction_is_finished).where(*loading_is_pending_or_failed)  # type: ignore
    if index_name:
        query = query.where(*index_name_is)  # type: ignore

    pending_tasks = query.limit(1).stream()  # type: ignore
    snapshot = next(pending_tasks, None)

    if not snapshot:
        return None

    data = snapshot.to_dict()
    assert data is not None

    task = Task.from_dict(data)
    transaction.update(snapshot.reference, task.update_on_loading_started(worker_id))
    return task


def _is_transient_error(error: Exception) -> bool:
    serialized = str(error).lower()

    if "please try again" in serialized:
        return True
    if "aborted due to cross-transaction contention" in serialized:
        return True
    if "failed to commit transaction" in serialized:
        return True

    return False


def _split_to_chunks(items: List[Any], chunk_size: int) -> List[List[Any]]:
    chunks: List[List[Any]] = []

    for i in range(0, len(items), chunk_size):
        chunk = items[i: i + chunk_size]
        chunks.append(chunk)

    return chunks
