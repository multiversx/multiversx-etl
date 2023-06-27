import logging
import socket
import time
from pathlib import Path
from typing import Callable, Optional

import click

from multiversxetl.errors import TransientError
from multiversxetl.jobs import ExtractJob, FileStorage, LoadJob, TransformJob
from multiversxetl.logger import CloudLogger
from multiversxetl.planner import (TasksStorage, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)

ERROR_INTERRUPTED_TASK = "interrupted task"


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by Elasticsearch index name (if omitted, all indices are extracted).")
@click.option("--continue-on-error", is_flag=True, type=bool, help="Continue (with other tasks) on error.")
@click.option("--sleep-between-tasks", type=int, default=3, help="Time to sleep between tasks (in seconds).")
def extract_with_intervals(
        workspace: str,
        gcp_project_id: str,
        worker_id: str,
        index_name: Optional[str],
        continue_on_error: bool,
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
        continue_on_error,
        sleep_between_tasks
    )


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--continue-on-error", is_flag=True, type=bool, help="Continue (with other tasks) on error.")
@click.option("--sleep-between-tasks", type=int, default=3, help="Time to sleep between tasks (in seconds).")
def extract_without_intervals(
        workspace: str,
        gcp_project_id: str,
        worker_id: str,
        index_name: Optional[str],
        continue_on_error: bool,
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
        continue_on_error,
        sleep_between_tasks
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

        extract_job = ExtractJob(file_storage, task)
        extract_job.run()
        storage.update_task(task.id, lambda t: t.update_on_extraction_finished(""))

        logger.log_info(f"Extraction finished, index = {task.index_name}, task = {task.id}", data=task.to_dict())
    except KeyboardInterrupt:
        storage.update_task(task.id, lambda t: t.update_on_extraction_failure(ERROR_INTERRUPTED_TASK))
        logger.log_error(f"Extraction interrupted, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        raise
    except Exception as e:
        storage.update_task(task.id, lambda t: t.update_on_extraction_failure(str(e)))
        logger.log_error(f"Extraction failed, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        raise


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--schema-folder", required=True, type=str, help="Folder with schema files.")
@click.option("--continue-on-error", is_flag=True, type=bool, help="Continue (with other tasks) on error.")
@click.option("--sleep-between-tasks", type=int, default=3, help="Time to sleep between tasks (in seconds).")
def load_with_intervals(
        workspace: str,
        gcp_project_id: str,
        worker_id: str,
        index_name: Optional[str],
        schema_folder: str,
        continue_on_error: bool,
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
        continue_on_error,
        sleep_between_tasks
    )


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--schema-folder", required=True, type=str, help="Folder with schema files.")
@click.option("--continue-on-error", is_flag=True, type=bool, help="Continue (with other tasks) on error.")
@click.option("--sleep-between-tasks", type=int, default=3, help="Time to sleep between tasks (in seconds).")
def load_without_intervals(
    workspace: str,
    gcp_project_id: str,
    worker_id: str,
    index_name: Optional[str],
    schema_folder: str,
    continue_on_error: bool,
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
        continue_on_error,
        sleep_between_tasks
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
    except KeyboardInterrupt:
        storage.update_task(task.id, lambda t: t.update_on_extraction_failure(ERROR_INTERRUPTED_TASK))
        logger.log_error(f"Transform & load interrupted, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        raise
    except Exception as e:
        storage.update_task(task.id, lambda t: t.update_on_loading_failure(str(e)))
        logger.log_error(f"Transform & load failed, index = {task.index_name}, task = {task.id}", data=task.to_dict())
        raise


def do_continuously(callable: Callable[[], None], continue_on_error: bool, sleep_between_tasks: int):
    while True:
        try:
            callable()
        except TransientError as error:
            logging.info(f"Transient error, will try again later: {error}")
        except:
            if not continue_on_error:
                raise

        logging.info(f"Sleeping for {sleep_between_tasks} seconds...")
        time.sleep(sleep_between_tasks)
