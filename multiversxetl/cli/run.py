import datetime
import logging
import sys
import time
from pathlib import Path
from typing import Optional, Tuple

import click

from multiversxetl.cli.checks import deduplicate_loaded_data
from multiversxetl.cli.plan_tasks import plan_tasks
from multiversxetl.cli.work import work_on_tasks
from multiversxetl.constants import (INDICES_WITH_INTERVALS,
                                     INDICES_WITHOUT_INTERVALS, SECONDS_IN_DAY)
from multiversxetl.planner.tasks_reporter import TasksReporter
from multiversxetl.planner.tasks_storage import (TasksStorage,
                                                 TasksWithIntervalStorage,
                                                 TasksWithoutIntervalStorage)


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, required=True, help="The GCP project ID.")
@click.option("--workspace", required=True, type=str, help="Workspace path.")
@click.option("--group", type=str, required=True, help="Tasks group (tag). Used as Firestore collections prefix.")
@click.option("--indexer-url", type=str, required=True, help="The indexer URL (Elasticsearch instance).")
@click.option("--planning-indices", multiple=True, default=INDICES_WITH_INTERVALS + INDICES_WITHOUT_INTERVALS)
@click.option("--bq-dataset", type=str, required=True, help="The BigQuery dataset (destination).")
@click.option("--initial-start-timestamp", type=int, help="The start timestamp (e.g. genesis time).")
@click.option("--initial-end-timestamp", type=int, help="The end timestamp (e.g. a recent one).")
@click.option("--tasks-granularity", type=int, default=SECONDS_IN_DAY, help="Task granularity, in seconds.")
@click.option("--worker-id", type=str, help="Worker ID (e.g. computer name).")
@click.option("--num-threads-per-work-job", type=int, default=1, help="Number of threads per job.")
@click.option("--schema-folder", required=True, type=str, help="Folder with schema files.")
@click.option("--sleep-between-tasks", type=int, default=3, help="Time to sleep between tasks within a job (in seconds).")
@click.option("--num-units-per-work-batch", type=int, default=100, help="Number of tasks to consume (handle) within a work batch.")
@click.option("--skip-planning", is_flag=True, help="Do not plan new tasks.")
@click.option("--skip-working", is_flag=True, help="Do not work (consume tasks).")
@click.option("--skip-deduplication", is_flag=True, help="Do not perform data deduplication.")
@click.option("--num-global-iterations", type=int, default=sys.maxsize, help="Number of global plan-work-deduplicate iterations.")
@click.option("--min-delay-between-global-iterations", type=int, default=3, help="Minimum delay between global plan-work-deduplicate iterations (in seconds).")
def run(
    gcp_project_id: str,
    workspace: str,
    group: str,
    indexer_url: str,
    planning_indices: Tuple[str, ...],
    bq_dataset: str,
    initial_start_timestamp: Optional[int],
    initial_end_timestamp: Optional[int],
    tasks_granularity: int,
    worker_id: str,
    num_threads_per_work_job: int,
    schema_folder: str,
    sleep_between_tasks: int,
    num_units_per_work_batch: int,
    skip_planning: bool,
    skip_working: bool,
    skip_deduplication: bool,
    num_global_iterations: int,
    min_delay_between_global_iterations: int
):
    start_time_global = _get_now()

    tasks_with_interval_storage = TasksWithIntervalStorage(gcp_project_id, group)
    tasks_without_interval_storage = TasksWithoutIntervalStorage(gcp_project_id, group)
    reports_folder_parent = Path(workspace) / group / "reports" / f"run_{_timestamp_to_filename_friendly_time(start_time_global)}"

    for i in range(num_global_iterations):
        start_time_of_iteration = _get_now()
        reports_folder = reports_folder_parent / f"iteration_{i:08}_{start_time_of_iteration}"

        _run_iteration(
            tasks_with_interval_storage,
            tasks_without_interval_storage,
            reports_folder,
            gcp_project_id,
            workspace,
            group,
            indexer_url,
            planning_indices,
            bq_dataset,
            initial_start_timestamp if i == 0 else None,
            initial_end_timestamp if i == 0 else None,
            tasks_granularity,
            worker_id,
            num_threads_per_work_job,
            schema_folder,
            sleep_between_tasks,
            num_units_per_work_batch,
            skip_planning,
            skip_working,
            skip_deduplication,
        )

        end_time_of_iteration = _get_now()
        time_elapsed_on_iteration = end_time_of_iteration - start_time_of_iteration
        time_to_sleep = min_delay_between_global_iterations - time_elapsed_on_iteration

        if time_to_sleep > 0:
            logging.info(f"Sleeping for {time_to_sleep} seconds...")
            time.sleep(time_to_sleep)


def _run_iteration(
    tasks_with_interval_storage: TasksStorage,
    tasks_without_interval_storage: TasksStorage,
    reports_folder: Path,
    gcp_project_id: str,
    workspace: str,
    group: str,
    indexer_url: str,
    planning_indices: Tuple[str, ...],
    bq_dataset: str,
    initial_start_timestamp: Optional[int],
    initial_end_timestamp: Optional[int],
    tasks_granularity: int,
    worker_id: str,
    num_threads_per_work_job: int,
    schema_folder: str,
    sleep_between_tasks: int,
    num_units_per_work_batch: int,
    skip_planning: bool,
    skip_working: bool,
    skip_deduplication: bool,
):
    reporter = TasksReporter(reports_folder)

    if not skip_planning:
        plan_tasks(
            gcp_project_id,
            group,
            indexer_url,
            planning_indices,
            bq_dataset,
            initial_start_timestamp,
            initial_end_timestamp,
            tasks_granularity
        )

        _report_tasks(
            "after_planning",
            reporter,
            tasks_with_interval_storage,
            tasks_without_interval_storage
        )

    if not skip_working:
        for i in range(sys.maxsize):
            still_work_to_do = work_on_tasks(
                gcp_project_id,
                workspace,
                group,
                worker_id,
                num_units_per_work_batch,
                num_threads_per_work_job,
                None,
                sleep_between_tasks,
                schema_folder,
                tasks_with_interval_storage,
                tasks_without_interval_storage
            )

            _report_tasks(
                f"after_batch_{i:08}",
                reporter,
                tasks_with_interval_storage,
                tasks_without_interval_storage
            )

            _cleanup_finished_tasks(
                tasks_with_interval_storage,
                tasks_without_interval_storage
            )

            _report_tasks(
                f"after_batch_{i:08}_after_cleanup",
                reporter,
                tasks_with_interval_storage,
                tasks_without_interval_storage
            )

            if not still_work_to_do:
                break

    if not skip_deduplication:
        deduplicate_loaded_data(
            gcp_project_id,
            bq_dataset
        )


def _report_tasks(
    name: str,
    reporter: TasksReporter,
    tasks_with_interval_storage: TasksStorage,
    tasks_without_interval_storage: TasksStorage,
):
    now = _get_now()
    existing_tasks_with_interval = tasks_with_interval_storage.get_all_tasks()
    existing_tasks_without_interval = tasks_without_interval_storage.get_all_tasks()

    reporter.generate_report(
        f"{name}_{now}",
        existing_tasks_with_interval,
        existing_tasks_without_interval
    )


def _cleanup_finished_tasks(
    tasks_with_interval_storage: TasksStorage,
    tasks_without_interval_storage: TasksStorage,
):
    now = _get_now()

    logging.info(f"Re-fetch tasks (in order to cleanup old tasks)...")

    existing_tasks_with_interval = tasks_with_interval_storage.get_all_tasks()
    existing_tasks_without_interval = tasks_without_interval_storage.get_all_tasks()

    logging.info(f"Cleanup old tasks...")

    tasks_to_remove = [task for task in existing_tasks_with_interval if task.is_finished_some_time_ago(now)]
    tasks_with_interval_storage.delete_tasks(tasks_to_remove)

    tasks_to_remove = [task for task in existing_tasks_without_interval if task.is_finished_some_time_ago(now)]
    tasks_without_interval_storage.delete_tasks(tasks_to_remove)


def _get_now():
    return int(datetime.datetime.utcnow().timestamp())


def _timestamp_to_filename_friendly_time(timestamp: int):
    return datetime.datetime.utcfromtimestamp(timestamp).strftime("%Y-%m-%d_%H-%M-%S")
