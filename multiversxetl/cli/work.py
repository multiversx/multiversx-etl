import logging
import socket

import click

from multiversxetl.jobs.extraction_job import ExtractionJob
from multiversxetl.planner import TasksWithIntervalStorage
from multiversxetl.planner.tasks import TaskStatus

logging.basicConfig(level=logging.INFO)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
def do_extract_with_intervals(gcp_project_id: str, worker_id: str):
    if not worker_id:
        worker_id = socket.gethostname()

    storage = TasksWithIntervalStorage(gcp_project_id)
    task = storage.assign_next_task_to_worker(worker_id)
    if not task:
        print("No tasks left, try again later.")
        return

    print(f"Assigned task: {task.id}")
    print(task.to_dict())

    job = ExtractionJob(task)
    job.run()

    storage.update_task_status(task.id, TaskStatus.EXTRACTED)
