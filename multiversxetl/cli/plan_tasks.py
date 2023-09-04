import datetime
import logging
from typing import List, Optional, Tuple

from multiversxetl.constants import (INDICES_WITH_INTERVALS,
                                     INDICES_WITHOUT_INTERVALS,
                                     MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION)
from multiversxetl.errors import UsageError
from multiversxetl.planner import (TasksPlanner, TasksWithIntervalStorage,
                                   TasksWithoutIntervalStorage)
from multiversxetl.planner.tasks import (Task, exclude_redundant_task_against,
                                         find_redundant_tasks)


def plan_tasks(
    gcp_project_id: str,
    group: str,
    indexer_url: str,
    indices: Tuple[str, ...],
    bq_dataset: str,
    initial_start_timestamp: Optional[int],
    initial_end_timestamp: Optional[int],
    granularity: int
):
    logging.info(f"Plan tasks, group = {group}, indexer = {indexer_url}, dataset = {bq_dataset}, initial start time = {initial_start_timestamp}, initial end time = ${initial_end_timestamp}, granularity = {granularity} ...")

    planner = TasksPlanner()
    indices_with_intervals = set(INDICES_WITH_INTERVALS) & set(indices)
    indices_without_intervals = set(INDICES_WITHOUT_INTERVALS) & set(indices)
    tasks_with_interval_storage = TasksWithIntervalStorage(gcp_project_id, group)
    tasks_without_interval_storage = TasksWithoutIntervalStorage(gcp_project_id, group)

    now = int(datetime.datetime.utcnow().timestamp())

    logging.info(f"Fetch tasks...")

    existing_tasks_with_interval = tasks_with_interval_storage.get_all_tasks()
    existing_tasks_without_interval = tasks_without_interval_storage.get_all_tasks()

    logging.info(f"Find and remove redundant tasks...")

    tasks_to_remove = find_redundant_tasks(existing_tasks_with_interval)
    tasks_with_interval_storage.delete_tasks(tasks_to_remove)

    tasks_to_remove = find_redundant_tasks(existing_tasks_without_interval)
    tasks_without_interval_storage.delete_tasks(tasks_to_remove)

    logging.info(f"Re-fetch tasks...")

    existing_tasks_with_interval = tasks_with_interval_storage.get_all_tasks()
    existing_tasks_without_interval = tasks_without_interval_storage.get_all_tasks()

    newly_planned_tasks_with_interval: List[Task] = []
    newly_planned_tasks_without_interval: List[Task] = []

    logging.info(f"Prepare new tasks...")

    # Handle indices with intervals
    for index_name in indices_with_intervals:
        # Each index has its own start timestamp (generally speaking, they will be the same)
        start_timestamp = decide_start_timestamp(existing_tasks_with_interval, index_name, initial_start_timestamp)
        # End timestamp is shared among all indices
        end_timestamp = decide_end_timestamp(initial_end_timestamp, now)

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

    logging.info(f"Prepared new tasks: with intervals = {len(newly_planned_tasks_with_interval)}, without intervals = {len(newly_planned_tasks_without_interval)}.")
    logging.info(f"Exclude new redundant tasks...")

    newly_planned_tasks_with_interval = exclude_redundant_task_against(newly_planned_tasks_with_interval, existing_tasks_with_interval)
    newly_planned_tasks_without_interval = exclude_redundant_task_against(newly_planned_tasks_without_interval, existing_tasks_without_interval)

    logging.info(f"New tasks upon exclusion: with intervals = {len(newly_planned_tasks_with_interval)}, without intervals = {len(newly_planned_tasks_without_interval)}.")
    logging.info(f"Add new non-redundant tasks...")

    tasks_with_interval_storage.add_tasks(newly_planned_tasks_with_interval)
    tasks_without_interval_storage.add_tasks(newly_planned_tasks_without_interval)


def decide_start_timestamp(existing_tasks: List[Task], index_name: str, explicit_start_timestamp: Optional[int]) -> int:
    if explicit_start_timestamp is not None:
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
