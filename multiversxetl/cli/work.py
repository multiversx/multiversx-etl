import datetime
import logging
import socket
import threading
from pathlib import Path
from typing import List, Optional

from multiversxetl.atomic import AtomicCounter
from multiversxetl.indexer import Indexer
from multiversxetl.jobs import ExtractJob, FileStorage, LoadJob, TransformJob
from multiversxetl.logger import CloudLogger
from multiversxetl.planner import TasksStorage
from multiversxetl.threading import do_until


def work_on_tasks(
    gcp_project_id: str,
    workspace: str,
    group: str,
    worker_id: str,
    num_work_units: int,
    num_threads_per_job: int,
    index_name: Optional[str],
    sleep_between_tasks: int,
    schema_folder: str,
    tasks_with_interval_storage: TasksStorage,
    tasks_without_interval_storage: TasksStorage,
) -> bool:
    work_units_counter = AtomicCounter(num_work_units)
    is_work_enough_for_now = work_units_counter.has_reached_upper_checkpoint
    on_error_happened: threading.Event = threading.Event()

    def job_extract_tasks_with_interval():
        do_until(
            lambda: do_any_extract_task(gcp_project_id, workspace, group, tasks_with_interval_storage, worker_id, index_name),
            is_work_enough_for_now,
            on_error_happened,
            work_units_counter,
            sleep_between_tasks,
            num_threads_per_job,
            thread_name_prefix="extract_tasks_with_interval"
        )

    def job_extract_tasks_without_interval():
        do_until(
            lambda: do_any_extract_task(gcp_project_id, workspace, group, tasks_without_interval_storage, worker_id, index_name),
            is_work_enough_for_now,
            on_error_happened,
            work_units_counter,
            sleep_between_tasks,
            num_threads_per_job,
            thread_name_prefix="extract_tasks_without_interval"
        )

    def job_load_tasks_with_interval():
        do_until(
            lambda: do_any_load_task(gcp_project_id, workspace, group, tasks_with_interval_storage, worker_id, index_name, schema_folder),
            is_work_enough_for_now,
            on_error_happened,
            work_units_counter,
            sleep_between_tasks,
            num_threads_per_job,
            thread_name_prefix="load_tasks_with_interval"
        )

    def job_load_tasks_without_interval():
        do_until(
            lambda: do_any_load_task(gcp_project_id, workspace, group, tasks_without_interval_storage, worker_id, index_name, schema_folder),
            is_work_enough_for_now,
            on_error_happened,
            work_units_counter,
            sleep_between_tasks,
            num_threads_per_job,
            thread_name_prefix="load_tasks_without_interval"
        )

    jobs: List[threading.Thread] = []

    job = threading.Thread(
        name=f"job_extract_tasks_with_interval",
        target=job_extract_tasks_with_interval
    )
    job.start()
    jobs.append(job)

    job = threading.Thread(
        name=f"job_extract_tasks_without_interval",
        target=job_extract_tasks_without_interval
    )
    job.start()
    jobs.append(job)

    job = threading.Thread(
        name=f"job_load_tasks_with_interval",
        target=job_load_tasks_with_interval
    )
    job.start()
    jobs.append(job)

    job = threading.Thread(
        name=f"job_load_tasks_without_interval",
        target=job_load_tasks_without_interval
    )
    job.start()
    jobs.append(job)

    for job in jobs:
        if job.is_alive():
            job.join()

    # True if more work might be necessary in the future.
    return is_work_enough_for_now.is_set() or on_error_happened.is_set()


def do_any_extract_task(
        gcp_project_id: str,
        workspace: str,
        group: str,
        storage: TasksStorage,
        worker_id: str,
        index_name: Optional[str]
) -> bool:
    file_storage = FileStorage(Path(workspace) / group)
    worker_id = worker_id or socket.gethostname()
    logger = CloudLogger(gcp_project_id, worker_id)

    task = storage.take_any_extract_task(worker_id, index_name)
    if not task:
        logging.info("No tasks left, try again later.")
        return False

    try:
        logger.log_info(f"Starting extraction, index = {task.index_name}, task = {task.id} ...", data=task.to_dict())

        indexer = Indexer(task.indexer_url)
        extract_job = ExtractJob(indexer, file_storage, task)
        extract_job.run()
        storage.update_task(task.id, lambda t: t.update_on_extraction_finished(""))

        logger.log_info(f"Extraction finished, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        return True
    except Exception as e:
        storage.update_task(task.id, lambda t: t.update_on_extraction_failure(str(e)))
        logger.log_error(f"Extraction failed, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        raise


def do_any_load_task(
        gcp_project_id: str,
        workspace: str,
        group: str,
        storage: TasksStorage,
        worker_id: str,
        index_name: Optional[str],
        schema_folder: str
) -> bool:
    file_storage = FileStorage(Path(workspace) / group)
    worker_id = worker_id or socket.gethostname()
    logger = CloudLogger(gcp_project_id, worker_id)

    task = storage.take_any_load_task(worker_id, index_name)
    if not task:
        logging.info("No tasks left, try again later.")
        return False

    try:
        logger.log_info(f"Starting transform & load, index = {task.index_name}, task = {task.id} ...", data=task.to_dict())

        transform_job = TransformJob(file_storage, task)
        transform_job.run()
        load_job = LoadJob(gcp_project_id, file_storage, task, Path(schema_folder).expanduser().resolve())
        load_job.run()
        storage.update_task(task.id, lambda t: t.update_on_loading_finished("", get_now()))

        file_storage.remove_extracted_file(task.get_pretty_name())
        file_storage.remove_transformed_file(task.get_pretty_name())

        logger.log_info(f"Transform & load finished, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        return True
    except Exception as e:
        storage.update_task(task.id, lambda t: t.update_on_loading_failure(str(e)))
        logger.log_error(f"Transform & load failed, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        raise


def get_now():
    return int(datetime.datetime.utcnow().timestamp())
