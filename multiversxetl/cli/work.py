import logging
import socket
from pathlib import Path
from typing import Optional

import click

from multiversxetl.jobs import ExtractJob, FileStorage, LoadJob, TransformJob
from multiversxetl.planner import (TasksStorage, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)

logging.basicConfig(level=logging.INFO)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", type=str, help="Workspace path.")
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
def do_extract_with_intervals(workspace: str, gcp_project_id: str, worker_id: str, index_name: Optional[str]):
    storage = TasksWithIntervalStorage(gcp_project_id)
    take_any_extract_task(workspace, storage, worker_id, index_name)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", type=str, help="Workspace path.")
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
def do_extract_without_intervals(workspace: str, gcp_project_id: str, worker_id: str, index_name: Optional[str]):
    storage = TasksWithoutIntervalStorage(gcp_project_id)
    take_any_extract_task(workspace, storage, worker_id, index_name)


def take_any_extract_task(workspace: str, storage: TasksStorage, worker_id: str, index_name: Optional[str]):
    file_storage = FileStorage(Path(workspace))
    worker_id = worker_id or socket.gethostname()

    task = storage.take_any_extract_task(worker_id, index_name)
    if not task:
        print("No tasks left, try again later.")
        return

    print(f"Assigned task: {task.id}")
    print(task.to_dict())

    extract_job = ExtractJob(file_storage, task)
    extract_job.run()

    storage.update_task(task.id, lambda t: t.update_on_extraction_finished({}))


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", type=str, help="Workspace path.")
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--schema-folder", type=str, help="Folder with schema files.")
def do_load_with_intervals(workspace: str, gcp_project_id: str, worker_id: str, index_name: Optional[str], schema_folder: str):
    storage = TasksWithIntervalStorage(gcp_project_id)
    take_any_load_task(workspace, storage, worker_id, index_name, schema_folder)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--workspace", type=str, help="Workspace path.")
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--index-name", type=str, help="Filter by index name.")
@click.option("--schema-folder", type=str, help="Folder with schema files.")
def do_load_without_intervals(workspace: str, gcp_project_id: str, worker_id: str, index_name: Optional[str], schema_folder: str):
    storage = TasksWithoutIntervalStorage(gcp_project_id)
    take_any_load_task(workspace, storage, worker_id, index_name, schema_folder)


def take_any_load_task(workspace: str, storage: TasksStorage, worker_id: str, index_name: Optional[str], schema_folder: str):
    file_storage = FileStorage(Path(workspace))
    worker_id = worker_id or socket.gethostname()

    task = storage.take_any_load_task(worker_id, index_name)
    if not task:
        print("No tasks left, try again later.")
        return

    print(f"Assigned task: {task.id}")
    print(task.to_dict())

    transform_job = TransformJob(file_storage, task)
    transform_job.run()

    load_job = LoadJob(file_storage, task, Path(schema_folder))
    load_job.run()
