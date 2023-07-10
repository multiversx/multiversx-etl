import datetime
from pprint import pprint
from typing import Tuple

import click

from multiversxetl.constants import (INDICES_WITH_INTERVALS,
                                     INDICES_WITHOUT_INTERVALS,
                                     MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION,
                                     SECONDS_IN_DAY)
from multiversxetl.errors import UsageError
from multiversxetl.planner import (TasksPlanner, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)
from multiversxetl.planner.tasks import count_tasks_by_status


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
def inspect_tasks(gcp_project_id: str):
    storage = TasksWithIntervalStorage(gcp_project_id)
    tasks = storage.get_all_tasks()
    by_extraction_status, by_loading_status = count_tasks_by_status(tasks)

    print(f"Tasks with interval: {len(tasks)}")
    print("\tBy extraction status:")
    pprint(by_extraction_status, indent=4)
    print("\tBy loading status:")
    pprint(by_loading_status, indent=4)

    storage = TasksWithoutIntervalStorage(gcp_project_id)
    tasks = storage.get_all_tasks()
    by_extraction_status, by_loading_status = count_tasks_by_status(tasks)

    print(f"Tasks without interval: {len(tasks)}")
    print("\tBy extraction status:")
    pprint(by_extraction_status, indent=4)
    print("\tBy loading status:")
    pprint(by_loading_status, indent=4)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--indexer-url", type=str, help="The indexer URL (Elasticsearch instance).")
@click.option("--indices", multiple=True, default=INDICES_WITH_INTERVALS)
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--bq-dataset", type=str, required=True, help="The BigQuery dataset (destination).")
@click.option("--start-timestamp", type=int, help="The start timestamp (e.g. genesis time).")
@click.option("--end-timestamp", type=int, help="The end timestamp (e.g. a recent one).")
@click.option("--granularity", type=int, default=SECONDS_IN_DAY, help="Task granularity, in seconds.")
def plan_tasks_with_intervals(indexer_url: str, indices: Tuple[str, ...], gcp_project_id: str, bq_dataset: str, start_timestamp: int, end_timestamp: int, granularity: int):
    storage = TasksWithIntervalStorage(gcp_project_id)
    planner = TasksPlanner()

    end_timestamp = decide_end_timestamp(end_timestamp)

    new_tasks = planner.plan_tasks_with_intervals(
        indexer_url,
        list(indices),
        bq_dataset,
        start_timestamp,
        end_timestamp,
        granularity
    )

    storage.add_tasks(new_tasks)


def decide_end_timestamp(end_timestamp: int):
    now = int(datetime.datetime.utcnow().timestamp())
    max_end_timestamp = now - MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION

    if not end_timestamp:
        end_timestamp = max_end_timestamp
    elif end_timestamp > max_end_timestamp:
        raise UsageError(f"End timestamp {end_timestamp} is too recent. It should be at most {max_end_timestamp} (current time - {MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION} seconds).")

    return end_timestamp


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--indexer-url", type=str, help="The indexer URL (Elasticsearch instance).")
@click.option("--indices", multiple=True, default=INDICES_WITHOUT_INTERVALS)
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--bq-dataset", type=str, help="The BigQuery dataset (destination).")
def plan_tasks_without_intervals(indexer_url: str, indices: Tuple[str, ...], gcp_project_id: str, bq_dataset: str):
    storage = TasksWithoutIntervalStorage(gcp_project_id)
    planner = TasksPlanner()
    new_tasks = planner.plan_tasks_without_intervals(indexer_url, list(indices), bq_dataset)
    storage.add_tasks(new_tasks)
