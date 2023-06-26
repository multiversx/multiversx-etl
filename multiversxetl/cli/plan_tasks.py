import datetime
import logging
from pprint import pprint

import click

from multiversxetl.constants import SECONDS_IN_DAY, SECONDS_IN_MINUTE
from multiversxetl.planner import (TasksPlanner, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)
from multiversxetl.planner.tasks import count_tasks_by_status

logging.basicConfig(level=logging.INFO)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
def inspect_tasks(gcp_project_id: str):
    storage = TasksWithIntervalStorage(gcp_project_id)
    tasks = storage.get_all_tasks()
    print(f"Tasks with interval: {len(tasks)}")
    by_extraction_status, by_loading_status = count_tasks_by_status(tasks)
    print("\tBy extraction status:")
    pprint(by_extraction_status, indent=4)
    print("\tBy loading status:")
    pprint(by_loading_status, indent=4)

    storage = TasksWithoutIntervalStorage(gcp_project_id)
    tasks = storage.get_all_tasks()
    print(f"Tasks without interval: {len(tasks)}")
    by_extraction_status, by_loading_status = count_tasks_by_status(tasks)
    print("\tBy extraction status:")
    pprint(by_extraction_status, indent=4)
    print("\tBy loading status:")
    pprint(by_loading_status, indent=4)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--indexer-url", type=str, help="The indexer URL (Elasticsearch instance).")
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--bq-dataset", type=str, required=True, help="The BigQuery dataset (destination).")
@click.option("--start-timestamp", type=int, help="The start timestamp (e.g. genesis time).")
@click.option("--end-timestamp", type=int, help="The end timestamp (e.g. a recent one).")
@click.option("--granularity", type=int, default=SECONDS_IN_DAY, help="Task granularity, in seconds.")
def plan_tasks_with_intervals(indexer_url: str, gcp_project_id: str, bq_dataset: str, start_timestamp: int, end_timestamp: int, granularity: int):
    storage = TasksWithIntervalStorage(gcp_project_id)
    planner = TasksPlanner()

    if not end_timestamp:
        now = int(datetime.datetime.utcnow().timestamp())
        end_timestamp = now - 25 * SECONDS_IN_MINUTE

    new_tasks = planner.plan_tasks_with_intervals(
        indexer_url,
        bq_dataset,
        start_timestamp,
        end_timestamp,
        granularity
    )

    storage.add_tasks(new_tasks)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--indexer-url", type=str, help="The indexer URL (Elasticsearch instance).")
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--bq-dataset", type=str, help="The BigQuery dataset (destination).")
def plan_tasks_without_intervals(indexer_url: str, gcp_project_id: str, bq_dataset: str):
    storage = TasksWithoutIntervalStorage(gcp_project_id)
    planner = TasksPlanner()
    new_tasks = planner.plan_tasks_without_intervals(indexer_url, bq_dataset)
    storage.add_tasks(new_tasks)
