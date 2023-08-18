import datetime
import logging
import time
from pprint import pprint
from typing import List, Optional, Tuple

import click

from multiversxetl.constants import (INDICES_WITH_INTERVALS,
                                     INDICES_WITHOUT_INTERVALS,
                                     MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION,
                                     SECONDS_IN_DAY)
from multiversxetl.errors import UsageError
from multiversxetl.planner import (TasksPlanner, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)
from multiversxetl.planner.tasks import (Task, count_tasks_by_status,
                                         group_tasks_by_index_name)
from multiversxetl.planner.tasks_storage import TasksStorage


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, required=True, help="The GCP project ID.")
@click.option("--group", type=str, required=True, help="Tasks group (tag). Used as Firestore collections prefix.")
@click.option("--indexer-url", type=str, required=True, help="The indexer URL (Elasticsearch instance).")
@click.option("--indices", multiple=True, default=INDICES_WITH_INTERVALS + INDICES_WITHOUT_INTERVALS)
@click.option("--bq-dataset", type=str, required=True, help="The BigQuery dataset (destination).")
@click.option("--initial_start-timestamp", type=int, help="The start timestamp (e.g. genesis time).")
@click.option("--initial-end-timestamp", type=int, help="The end timestamp (e.g. a recent one).")
@click.option("--granularity", type=int, default=SECONDS_IN_DAY, help="Task granularity, in seconds.")
@click.option("--num-repeats", type=int, default=1, help="Number of repeats.")
@click.option("--sleep-between-repeats", type=int, default=SECONDS_IN_DAY, help="Time to sleep between planning sessions (repeats).")
def plan_tasks(
    gcp_project_id: str,
    group: str,
    indexer_url: str,
    indices: Tuple[str, ...],
    bq_dataset: str,
    initial_start_timestamp: Optional[int],
    initial_end_timestamp: Optional[int],
    granularity: int,
    num_repeats: bool,
    sleep_between_repeats: int
):
    planner = TasksPlanner()
    indices_with_intervals = set(INDICES_WITH_INTERVALS) & set(indices)
    indices_without_intervals = set(INDICES_WITHOUT_INTERVALS) & set(indices)
    tasks_with_interval_storage = TasksWithIntervalStorage(gcp_project_id, group)
    tasks_without_interval_storage = TasksWithoutIntervalStorage(gcp_project_id, group)

    newly_planned_tasks_with_interval: List[Task] = []
    newly_planned_tasks_without_interval: List[Task] = []

    for i in range(num_repeats):
        # Handle indices with intervals
        for index_name in indices_with_intervals:
            # Each index has its own start timestamp (generally speaking, they will be the same)
            start_timestamp = decide_start_timestamp(tasks_with_interval_storage, index_name, initial_start_timestamp if i == 0 else None)
            # End timestamp is shared among all indices
            end_timestamp = decide_end_timestamp(initial_end_timestamp if i == 0 else None)

            new_tasks = planner.plan_tasks_with_intervals(
                indexer_url,
                index_name,
                bq_dataset,
                start_timestamp,
                end_timestamp,
                granularity
            )

            newly_planned_tasks_with_interval.extend(new_tasks)

        # Handle indices without intervals
        for index_name in indices_without_intervals:
            new_tasks = planner.plan_tasks_without_intervals(indexer_url, index_name, bq_dataset)
            newly_planned_tasks_without_interval.extend(new_tasks)

        # Store the newly planned tasks
        tasks_with_interval_storage.add_tasks(newly_planned_tasks_with_interval)
        tasks_without_interval_storage.add_tasks(newly_planned_tasks_without_interval)

        logging.info(f"Sleeping for {sleep_between_repeats} seconds...")
        time.sleep(sleep_between_repeats)


def decide_start_timestamp(storage: TasksStorage, index_name: str, explicit_start_timestamp: Optional[int]) -> int:
    if explicit_start_timestamp:
        return explicit_start_timestamp

    latest_task = storage.find_latest_task(index_name)
    if not latest_task:
        raise UsageError(f"Cannot find any previous task for index = {index_name}. Please specify a start timestamp explicitly.")

    latest_task_end_timestamp = latest_task.end_timestamp
    assert latest_task_end_timestamp is not None
    return latest_task_end_timestamp


def decide_end_timestamp(explicit_end_timestamp: Optional[int]):
    now = int(datetime.datetime.utcnow().timestamp())
    max_end_timestamp = now - MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION

    if not explicit_end_timestamp:
        return max_end_timestamp
    if explicit_end_timestamp > max_end_timestamp:
        raise UsageError(f"End timestamp {explicit_end_timestamp} is too recent. It should be at most {max_end_timestamp} (current time - {MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION} seconds).")

    return explicit_end_timestamp


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--group", type=str, required=True, help="Tasks group (tag). Used as Firestore collections prefix.")
@click.option("--sleep-interval", type=int, default=SECONDS_IN_DAY, help="Time to sleep between planning sessions.")
def inspect_tasks(gcp_project_id: str, group: str, sleep_interval: int):
    storage = TasksWithIntervalStorage(gcp_project_id, group)
    tasks = storage.get_all_tasks()

    print(f"Tasks with interval: {len(tasks)}")
    display_tasks(tasks)

    storage = TasksWithoutIntervalStorage(gcp_project_id, group)
    tasks = storage.get_all_tasks()

    print(f"Tasks without interval: {len(tasks)}")
    display_tasks(tasks)


def display_tasks(tasks: List[Task]):
    counts_by_extraction_status, counts_by_loading_status = count_tasks_by_status(tasks)
    tasks_by_index_name = group_tasks_by_index_name(tasks)

    print("By extraction status:")
    pprint(counts_by_extraction_status, indent=4)
    print("By loading status:")
    pprint(counts_by_loading_status, indent=4)

    print("Details:")

    for tasks in tasks_by_index_name.values():
        for task in tasks:
            start = datetime.datetime.utcfromtimestamp(task.start_timestamp) if task.start_timestamp else None
            end = datetime.datetime.utcfromtimestamp(task.end_timestamp) if task.end_timestamp else None

            print(f"ID = {task.id}, index = {task.index_name}, start = {start}, end = {end}, status = [{task.loading_status}, {task.extraction_status}]")
