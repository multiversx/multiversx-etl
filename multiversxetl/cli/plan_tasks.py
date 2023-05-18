import datetime
import logging

import click

from multiversxetl.planner import TasksPlanner, TasksStorage

SECONDS_IN_MINUTE = 60
SECONDS_IN_DAY = 24 * 60 * SECONDS_IN_MINUTE

logging.basicConfig(level=logging.INFO)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
def inspect_tasks(gcp_project_id: str):
    storage = TasksStorage(gcp_project_id)
    tasks = storage.get_all_tasks()
    print(f"Total tasks: {len(tasks)}")


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--indexer-url", type=str, help="The indexer URL (Elastic Search instance).")
@click.option("--start-timestamp", type=int, help="The start timestamp (e.g. genesis time).")
@click.option("--end-timestamp", type=int, help="The end timestamp (e.g. a recent one).")
@click.option("--granularity", type=int, default=SECONDS_IN_DAY, help="Task granularity, in seconds.")
def plan_tasks_with_intervals(gcp_project_id: str, indexer_url: str, start_timestamp: int, end_timestamp: int, granularity: int):
    storage = TasksStorage(gcp_project_id)
    planner = TasksPlanner()

    if not end_timestamp:
        now = int(datetime.datetime.utcnow().timestamp())
        end_timestamp = now - 25 * SECONDS_IN_MINUTE

    new_tasks = planner.plan_tasks_with_intervals(
        indexer_url,
        start_timestamp,
        end_timestamp,
        granularity
    )

    storage.add_tasks(new_tasks)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--indexer-url", type=str, help="The indexer URL (Elastic Search instance).")
def plan_tasks_without_intervals(gcp_project_id: str, indexer_url: str):
    storage = TasksStorage(gcp_project_id)
    planner = TasksPlanner()
    new_tasks = planner.plan_tasks_without_intervals(indexer_url)
    storage.add_tasks(new_tasks)
