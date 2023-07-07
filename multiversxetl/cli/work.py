import logging
import socket
import threading
import time
from pathlib import Path
from typing import Callable, List, Optional

import click

from multiversxetl.errors import TransientError
from multiversxetl.indexer import Indexer
from multiversxetl.jobs import ExtractJob, FileStorage, LoadJob, TransformJob
from multiversxetl.logger import CloudLogger
from multiversxetl.planner import (TasksStorage, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--num-threads", type=int, default=1, help="Number of threads.")
@click.option("--index-name", type=str, help="Filter by Elasticsearch index name (if omitted, all indices are extracted).")
@click.option("--sleep-between-tasks", type=int, default=3, help="Time to sleep between tasks (in seconds).")
def extract_with_intervals(
        workspace: str,
        gcp_project_id: str,
        worker_id: str,
        num_threads: int,
        index_name: Optional[str],
        sleep_between_tasks: int
):
    """
    This command runs continuously (until interrupted) and extracts **time-stamped indices** from Elasticsearch, 
    then saves them (as JSON files) in the `workspace` folder.

    Example of **time-stamped indices**: blocks, miniblocks, transactions etc.
    """

    storage = TasksWithIntervalStorage(gcp_project_id)

    do_continuously(
        lambda: do_any_extract_task(workspace, gcp_project_id, storage, worker_id, index_name),
        sleep_between_tasks,
        num_threads
    )


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--num-threads", type=int, default=1, help="Number of threads.")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--sleep-between-tasks", type=int, default=3, help="Time to sleep between tasks (in seconds).")
def extract_without_intervals(
        workspace: str,
        gcp_project_id: str,
        worker_id: str,
        num_threads: int,
        index_name: Optional[str],
        sleep_between_tasks: int
):
    """
    This command runs continuously (until interrupted) and extracts **not-time-stamped indices** from Elasticsearch,
    then saves them (as JSON files) in the `workspace` folder.

    Example of **not-time-stamped indices**: accounts, validators, delegators etc.
    """

    storage = TasksWithoutIntervalStorage(gcp_project_id)

    do_continuously(
        lambda: do_any_extract_task(workspace, gcp_project_id, storage, worker_id, index_name),
        sleep_between_tasks,
        num_threads
    )


def do_any_extract_task(
        workspace: str,
        gcp_project_id: str,
        storage: TasksStorage,
        worker_id: str,
        index_name: Optional[str]
):
    file_storage = FileStorage(Path(workspace))
    worker_id = worker_id or socket.gethostname()
    logger = CloudLogger(gcp_project_id, worker_id)

    task = storage.take_any_extract_task(worker_id, index_name)
    if not task:
        logging.info("No tasks left, try again later.")
        return

    try:
        logger.log_info(f"Starting extraction, index = {task.index_name}, task = {task.id} ...", data=task.to_dict())

        indexer = Indexer(task.indexer_url)
        extract_job = ExtractJob(indexer, file_storage, task)
        extract_job.run()
        storage.update_task(task.id, lambda t: t.update_on_extraction_finished(""))

        logger.log_info(f"Extraction finished, index = {task.index_name}, task = {task.id}", data=task.to_dict())
    except Exception as e:
        storage.update_task(task.id, lambda t: t.update_on_extraction_failure(str(e)))
        logger.log_error(f"Extraction failed, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        raise


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--num-threads", type=int, default=1, help="Number of threads.")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--schema-folder", required=True, type=str, help="Folder with schema files.")
@click.option("--sleep-between-tasks", type=int, default=3, help="Time to sleep between tasks (in seconds).")
def load_with_intervals(
        workspace: str,
        gcp_project_id: str,
        worker_id: str,
        num_threads: int,
        index_name: Optional[str],
        schema_folder: str,
        sleep_between_tasks: int
):
    """
    This command runs continuously (until interrupted) and loads **time-stamped indices** from the `workspace` folder
    into a BigQuery dataset.

    Before loading, the data suffers slight transformations (when needed).
    """

    storage = TasksWithIntervalStorage(gcp_project_id)

    do_continuously(
        lambda: do_any_load_task(workspace, gcp_project_id, storage, worker_id, index_name, schema_folder),
        sleep_between_tasks,
        num_threads
    )


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--num-threads", type=int, default=1, help="Number of threads.")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--schema-folder", required=True, type=str, help="Folder with schema files.")
@click.option("--sleep-between-tasks", type=int, default=3, help="Time to sleep between tasks (in seconds).")
def load_without_intervals(
    workspace: str,
    gcp_project_id: str,
    worker_id: str,
    num_threads: int,
    index_name: Optional[str],
    schema_folder: str,
    sleep_between_tasks: int
):
    """
    This command runs continuously (until interrupted) and loads **non-time-stamped indices** from the `workspace` folder
    into a BigQuery dataset.

    Before loading, the data suffers slight transformations (when needed).
    """
    storage = TasksWithoutIntervalStorage(gcp_project_id)

    do_continuously(
        lambda: do_any_load_task(workspace, gcp_project_id, storage, worker_id, index_name, schema_folder),
        sleep_between_tasks,
        num_threads
    )


def do_any_load_task(
        workspace: str,
        gcp_project_id: str,
        storage: TasksStorage,
        worker_id: str,
        index_name: Optional[str],
        schema_folder: str
):
    file_storage = FileStorage(Path(workspace))
    worker_id = worker_id or socket.gethostname()
    logger = CloudLogger(gcp_project_id, worker_id)

    task = storage.take_any_load_task(worker_id, index_name)
    if not task:
        logging.info("No tasks left, try again later.")
        return

    try:
        logger.log_info(f"Starting transform & load, index = {task.index_name}, task = {task.id} ...", data=task.to_dict())

        transform_job = TransformJob(file_storage, task)
        transform_job.run()
        load_job = LoadJob(gcp_project_id, file_storage, task, Path(schema_folder))
        load_job.run()
        storage.update_task(task.id, lambda t: t.update_on_loading_finished(""))
        file_storage.remove_transformed_file(task.get_pretty_name())

        logger.log_info(f"Transform & load finished, index = {task.index_name}, task = {task.id}", data=task.to_dict())
    except Exception as e:
        storage.update_task(task.id, lambda t: t.update_on_loading_failure(str(e)))
        logger.log_error(f"Transform & load failed, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        raise


def do_continuously(callable: Callable[[], None], sleep_between_tasks: int, num_threads: int, thread_start_delay: int = 1):
    threads: List[threading.Thread] = []
    should_stop_all_threads = threading.Event()

    for i in range(num_threads):
        # Start threads with a small delay between them.
        time.sleep(thread_start_delay)

        thread = threading.Thread(
            name=f"Thread-{i}",
            target=do_continuously_in_thread,
            args=[callable, sleep_between_tasks, should_stop_all_threads]
        )

        thread.start()
        threads.append(thread)

        if should_stop_all_threads.is_set():
            break

    for thread in threads:
        if thread.is_alive():
            thread.join()


def do_continuously_in_thread(callable: Callable[[], None], sleep_between_tasks: int, should_stop: threading.Event):
    logging.info(f"Thread started.")

    while not should_stop.is_set():
        try:
            callable()
        except TransientError as error:
            logging.info(f"Transient error, will try again later: {error}")
        except:
            should_stop.set()
            raise

        # Other threads may have set the stop flag (e.g. due to an exception).
        if should_stop.is_set():
            break

        logging.info(f"Sleeping for {sleep_between_tasks} seconds...")
        time.sleep(sleep_between_tasks)

    logging.info(f"Thread stopped.")
