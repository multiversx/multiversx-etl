import datetime
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


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--group", type=str, required=True, help="Tasks group (tag). Used as Firestore collections prefix.")
def inspect_tasks(gcp_project_id: str, group: str):
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


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, required=True, help="The GCP project ID.")
@click.option("--group", type=str, required=True, help="Tasks group (tag). Used as Firestore collections prefix.")
@click.option("--indexer-url", type=str, required=True, help="The indexer URL (Elasticsearch instance).")
@click.option("--indices", multiple=True, default=INDICES_WITH_INTERVALS)
@click.option("--bq-dataset", type=str, required=True, help="The BigQuery dataset (destination).")
@click.option("--start-timestamp", type=int, help="The start timestamp (e.g. genesis time).")
@click.option("--end-timestamp", type=int, help="The end timestamp (e.g. a recent one).")
@click.option("--granularity", type=int, default=SECONDS_IN_DAY, help="Task granularity, in seconds.")
def plan_tasks_with_intervals(
    gcp_project_id: str,
    group: str,
    indexer_url: str,
    indices: Tuple[str, ...],
    bq_dataset: str,
    start_timestamp: Optional[int],
    end_timestamp: Optional[int],
    granularity: int
):
    storage = TasksWithIntervalStorage(gcp_project_id, group)
    planner = TasksPlanner()
    newly_planned_tasks: List[Task] = []

    end_timestamp = decide_end_timestamp(end_timestamp)

    for index_name in list(indices):
        start_timestamp_of_index = decide_start_timestamp(storage, index_name, start_timestamp)

        start = datetime.datetime.utcfromtimestamp(start_timestamp_of_index)
        end = datetime.datetime.utcfromtimestamp(end_timestamp)
        print(f"Planning tasks for index = {index_name}, start = {start}, end = {end} ...")

        tasks = planner.plan_tasks_with_intervals(
            indexer_url,
            index_name,
            bq_dataset,
            start_timestamp_of_index,
            end_timestamp,
            granularity
        )

        newly_planned_tasks.extend(tasks)

    storage.add_tasks(newly_planned_tasks)


def decide_start_timestamp(storage: TasksWithIntervalStorage, index_name: str, start_timestamp: Optional[int]) -> int:
    if start_timestamp is not None:
        return start_timestamp

    latest_task = storage.find_latest_task(index_name)
    if not latest_task:
        raise UsageError(f"Cannot find any previous task for index = {index_name}. Please specify a start timestamp explicitly.")

    latest_task_end_timestamp = latest_task.end_timestamp
    assert latest_task_end_timestamp is not None
    return latest_task_end_timestamp


def decide_end_timestamp(end_timestamp: Optional[int]):
    now = int(datetime.datetime.utcnow().timestamp())
    max_end_timestamp = now - MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION

    if not end_timestamp:
        end_timestamp = max_end_timestamp
    elif end_timestamp > max_end_timestamp:
        raise UsageError(f"End timestamp {end_timestamp} is too recent. It should be at most {max_end_timestamp} (current time - {MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION} seconds).")

    return end_timestamp


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, required=True, help="The GCP project ID.")
@click.option("--group", type=str, required=True, help="Tasks group (tag). Used as Firestore collections prefix.")
@click.option("--indexer-url", type=str, required=True, help="The indexer URL (Elasticsearch instance).")
@click.option("--indices", multiple=True, default=INDICES_WITHOUT_INTERVALS)
@click.option("--bq-dataset", type=str, required=True, help="The BigQuery dataset (destination).")
def plan_tasks_without_intervals(
    gcp_project_id: str,
    group: str,
    indexer_url: str,
    indices: Tuple[str, ...],
    bq_dataset: str
):
    storage = TasksWithoutIntervalStorage(gcp_project_id, group)
    planner = TasksPlanner()
    newly_planned_tasks: List[Task] = []

    for index_name in list(indices):
        new_tasks = planner.plan_tasks_without_intervals(indexer_url, index_name, bq_dataset)
        newly_planned_tasks.extend(new_tasks)

    storage.add_tasks(newly_planned_tasks)
