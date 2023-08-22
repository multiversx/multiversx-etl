import datetime
import logging
import time
from pathlib import Path
from typing import List, Optional, Tuple

import click

from multiversxetl.constants import (INDICES_WITH_INTERVALS,
                                     INDICES_WITHOUT_INTERVALS,
                                     MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION,
                                     SECONDS_IN_DAY)
from multiversxetl.errors import UsageError
from multiversxetl.planner import (TasksPlanner, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)
from multiversxetl.planner.tasks import (Task,
                                         exclude_tasks_duplicates_against,
                                         find_tasks_duplicates_within)
from multiversxetl.planner.tasks_reporter import TasksReporter


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, required=True, help="The GCP project ID.")
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--group", type=str, required=True, help="Tasks group (tag). Used as Firestore collections prefix.")
@click.option("--indexer-url", type=str, required=True, help="The indexer URL (Elasticsearch instance).")
@click.option("--indices", multiple=True, default=INDICES_WITH_INTERVALS + INDICES_WITHOUT_INTERVALS)
@click.option("--bq-dataset", type=str, required=True, help="The BigQuery dataset (destination).")
@click.option("--initial-start-timestamp", type=int, help="The start timestamp (e.g. genesis time).")
@click.option("--initial-end-timestamp", type=int, help="The end timestamp (e.g. a recent one).")
@click.option("--granularity", type=int, default=SECONDS_IN_DAY, help="Task granularity, in seconds.")
@click.option("--num-repeats", type=int, default=1, help="Number of repeats.")
@click.option("--sleep-between-repeats", type=int, default=SECONDS_IN_DAY, help="Time to sleep between planning sessions (repeats).")
def plan_tasks(
    gcp_project_id: str,
    workspace: str,
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
    reporter = TasksReporter(Path(workspace) / group)
    indices_with_intervals = set(INDICES_WITH_INTERVALS) & set(indices)
    indices_without_intervals = set(INDICES_WITHOUT_INTERVALS) & set(indices)
    tasks_with_interval_storage = TasksWithIntervalStorage(gcp_project_id, group)
    tasks_without_interval_storage = TasksWithoutIntervalStorage(gcp_project_id, group)

    for i in range(num_repeats):
        now = int(datetime.datetime.utcnow().timestamp())

        existing_tasks_with_interval = tasks_with_interval_storage.get_all_tasks()
        existing_tasks_without_interval = tasks_without_interval_storage.get_all_tasks()

        # Remove duplicated tasks
        tasks_to_remove = find_tasks_duplicates_within(existing_tasks_with_interval)
        tasks_with_interval_storage.delete_tasks(tasks_to_remove)

        tasks_to_remove = find_tasks_duplicates_within(existing_tasks_without_interval)
        tasks_without_interval_storage.delete_tasks(tasks_to_remove)

        # Re-fetch tasks
        existing_tasks_with_interval = tasks_with_interval_storage.get_all_tasks()
        existing_tasks_without_interval = tasks_without_interval_storage.get_all_tasks()

        newly_planned_tasks_with_interval: List[Task] = []
        newly_planned_tasks_without_interval: List[Task] = []

        # Handle indices with intervals
        for index_name in indices_with_intervals:
            # Each index has its own start timestamp (generally speaking, they will be the same)
            start_timestamp = decide_start_timestamp(existing_tasks_with_interval, index_name, initial_start_timestamp if i == 0 else None)
            # End timestamp is shared among all indices
            end_timestamp = decide_end_timestamp(initial_end_timestamp if i == 0 else None, now)

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
        newly_planned_tasks_with_interval = exclude_tasks_duplicates_against(newly_planned_tasks_with_interval, existing_tasks_with_interval)
        newly_planned_tasks_without_interval = exclude_tasks_duplicates_against(newly_planned_tasks_without_interval, existing_tasks_without_interval)

        tasks_with_interval_storage.add_tasks(newly_planned_tasks_with_interval)
        tasks_without_interval_storage.add_tasks(newly_planned_tasks_without_interval)

        # Cleanup tasks
        existing_tasks_with_interval = tasks_with_interval_storage.get_all_tasks()
        existing_tasks_without_interval = tasks_without_interval_storage.get_all_tasks()

        for task in existing_tasks_with_interval:
            if task.is_finished_long_time_ago(now):
                tasks_with_interval_storage.delete_task(task.id)

        for task in existing_tasks_without_interval:
            if task.is_finished_long_time_ago(now):
                tasks_without_interval_storage.delete_task(task.id)

        existing_tasks_with_interval = tasks_with_interval_storage.get_all_tasks()
        existing_tasks_without_interval = tasks_without_interval_storage.get_all_tasks()

        reporter.generate_report(
            f"after_planning_{i:08}_{now}",
            existing_tasks_with_interval,
            existing_tasks_without_interval
        )

        logging.info(f"Sleeping for {sleep_between_repeats} seconds...")
        time.sleep(sleep_between_repeats)


def decide_start_timestamp(existing_tasks: List[Task], index_name: str, explicit_start_timestamp: Optional[int]) -> int:
    if explicit_start_timestamp:
        return explicit_start_timestamp

    sorted_tasks = sorted(existing_tasks, key=lambda task: task.end_timestamp or 0, reverse=True)
    filtered_tasks = [task for task in sorted_tasks if task.index_name == index_name]
    latest_task = filtered_tasks[0] if filtered_tasks else None

    if not latest_task:
        raise UsageError(f"Cannot find any previous task for index = {index_name}. Please specify a start timestamp explicitly.")

    latest_task_end_timestamp = latest_task.end_timestamp
    assert latest_task_end_timestamp is not None
    return latest_task_end_timestamp


def decide_end_timestamp(explicit_end_timestamp: Optional[int], now: int):
    max_end_timestamp = now - MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION

    if not explicit_end_timestamp:
        return max_end_timestamp
    if explicit_end_timestamp > max_end_timestamp:
        raise UsageError(f"End timestamp {explicit_end_timestamp} is too recent. It should be at most {max_end_timestamp} (current time - {MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION} seconds).")

    return explicit_end_timestamp
