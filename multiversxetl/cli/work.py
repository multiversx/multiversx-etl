import logging
import socket

import click

from multiversxetl.jobs.extraction_job import ExtractionJob
from multiversxetl.planner import (TasksStorage, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)

logging.basicConfig(level=logging.INFO)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
def do_extract_with_intervals(gcp_project_id: str, worker_id: str):
    worker_id = worker_id or socket.gethostname()
    storage = TasksWithIntervalStorage(gcp_project_id)
    start_any_extraction_task(storage, worker_id)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
def do_load_with_intervals(gcp_project_id: str, worker_id: str):
    worker_id = worker_id or socket.gethostname()
    storage = TasksWithIntervalStorage(gcp_project_id)
    task = storage.start_any_loading_task(worker_id)
    if not task:
        print("No tasks left, try again later.")
        return

    print(f"Assigned task: {task.id}")
    print(task.to_dict())


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
def do_extract_without_intervals(gcp_project_id: str, worker_id: str):
    worker_id = worker_id or socket.gethostname()
    storage = TasksWithoutIntervalStorage(gcp_project_id)
    start_any_extraction_task(storage, worker_id)


def start_any_extraction_task(storage: TasksStorage, worker_id: str):
    task = storage.start_any_extraction_task(worker_id)
    if not task:
        print("No tasks left, try again later.")
        return

    print(f"Assigned task: {task.id}")
    print(task.to_dict())

    job = ExtractionJob(task)
    job.run()

    storage.update_task(task.id, lambda t: t.update_on_extraction_finished({}))
