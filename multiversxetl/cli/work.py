import logging
import socket
import time
from pathlib import Path
from typing import Callable, Optional

import click

from multiversxetl.jobs import ExtractJob, FileStorage, LoadJob, TransformJob
from multiversxetl.planner import (TasksStorage, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)

logging.basicConfig(level=logging.INFO)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--continue-on-error", type=bool, help="Continue (with other tasks) on error.")
@click.option("--sleep-between-tasks", type=int, default=5, help="Time to sleep between tasks (in seconds).")
def do_extract_with_intervals(
        workspace: str,
        gcp_project_id: str,
        worker_id: str,
        index_name: Optional[str],
        continue_on_error: bool,
        sleep_between_tasks: int
):
    storage = TasksWithIntervalStorage(gcp_project_id)

    do_continuously(
        lambda: do_any_extract_task(workspace, storage, worker_id, index_name),
        continue_on_error,
        sleep_between_tasks
    )


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--continue-on-error", type=bool, help="Continue (with other tasks) on error.")
@click.option("--sleep-between-tasks", type=int, default=5, help="Time to sleep between tasks (in seconds).")
def do_extract_without_intervals(
        workspace: str,
        gcp_project_id: str,
        worker_id: str,
        index_name: Optional[str],
        continue_on_error: bool,
        sleep_between_tasks: int
):
    storage = TasksWithoutIntervalStorage(gcp_project_id)

    do_continuously(
        lambda: do_any_extract_task(workspace, storage, worker_id, index_name),
        continue_on_error,
        sleep_between_tasks
    )


def do_any_extract_task(
        workspace: str,
        storage: TasksStorage,
        worker_id: str,
        index_name: Optional[str]
):
    file_storage = FileStorage(Path(workspace))
    worker_id = worker_id or socket.gethostname()

    task = storage.take_any_extract_task(worker_id, index_name)
    if not task:
        print("No tasks left, try again later.")
        return

    print(f"Assigned task: {task.id}")
    print(task.to_dict())

    try:
        extract_job = ExtractJob(file_storage, task)
        extract_job.run()
        storage.update_task(task.id, lambda t: t.update_on_extraction_finished(""))
    except Exception as e:
        storage.update_task(task.id, lambda t: t.update_on_extraction_failure(str(e)))
        raise


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--gcp-project-id", required=True, type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--schema-folder", required=True, type=str, help="Folder with schema files.")
@click.option("--continue-on-error", type=bool, help="Continue (with other tasks) on error.")
@click.option("--sleep-between-tasks", type=int, default=5, help="Time to sleep between tasks (in seconds).")
def do_load_with_intervals(
        workspace: str,
        gcp_project_id: str,
        worker_id: str,
        index_name: Optional[str],
        schema_folder: str,
        continue_on_error: bool,
        sleep_between_tasks: int
):
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
@click.option("--continue-on-error", type=bool, help="Continue (with other tasks) on error.")
@click.option("--sleep-between-tasks", type=int, default=5, help="Time to sleep between tasks (in seconds).")
def do_load_without_intervals(
    workspace: str,
    gcp_project_id: str,
    worker_id: str,
    index_name: Optional[str],
    schema_folder: str,
    continue_on_error: bool,
    sleep_between_tasks: int
):
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

    task = storage.take_any_load_task(worker_id, index_name)
    if not task:
        print("No tasks left, try again later.")
        return

    print(f"Assigned task: {task.id}")
    print(task.to_dict())

    try:
        transform_job = TransformJob(file_storage, task)
        transform_job.run()

        load_job = LoadJob(gcp_project_id, file_storage, task, Path(schema_folder))
        load_job.run()
        storage.update_task(task.id, lambda t: t.update_on_loading_finished(""))
    except Exception as e:
        logging.exception(e)
        storage.update_task(task.id, lambda t: t.update_on_loading_failure(str(e)))
        raise


def do_continuously(callable: Callable[[], None], continue_on_error: bool, sleep_between_tasks: int):
    while True:
        try:
            callable()
        except:
            if not continue_on_error:
                raise

        print(f"Sleeping for {sleep_between_tasks} seconds...")
        time.sleep(sleep_between_tasks)
