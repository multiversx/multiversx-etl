import datetime
import logging
import os
import socket
import sys
import threading
import time
import traceback
from argparse import ArgumentParser
from pathlib import Path
from typing import List

import requests
from google.cloud import bigquery

from multiversxetl.checks import check_loaded_data
from multiversxetl.constants import (
    SECONDS_MIN_DELTA_BETWEEN_NOW_AND_ETL_ITERATION,
    SECONDS_SLEEP_BETWEEN_ETL_ITERATIONS)
from multiversxetl.errors import UsageError
from multiversxetl.file_storage import FileStorage
from multiversxetl.indexer import Indexer
from multiversxetl.logger import CloudLogger
from multiversxetl.tasks_dashboard import TasksDashboard
from multiversxetl.tasks_runner import TasksRunner
from multiversxetl.worker_config import WorkerConfig
from multiversxetl.worker_state import WorkerState

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s]: %(message)s")
# Suppress some logging from Elasticsearch.
logging.getLogger("elasticsearch.helpers").setLevel(logging.WARNING)
logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)


def main(args: List[str]) -> int:
    # See: https://github.com/grpc/grpc/issues/28557
    os.environ["GRPC_POLL_STRATEGY"] = "poll"

    try:
        _do_main(args)
        return 0
    except UsageError as error:
        logging.error(error)
        return 1
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
        return 1


def _do_main(args: List[str]):
    parser = ArgumentParser()
    parser.add_argument("--workspace", required=True, help="Workspace path.")

    parsed_args = parser.parse_args(args)

    workspace = Path(parsed_args.workspace).expanduser().resolve()
    worker_config_path = workspace / "worker_config.json"
    worker_state_path = workspace / "worker_state.json"

    if not worker_config_path.exists():
        raise UsageError(f"Worker config file not found: {worker_config_path}")
    if not worker_state_path.exists():
        raise UsageError(f"Worker state file not found: {worker_state_path}")

    for iteration_index in range(0, sys.maxsize):
        logging.info(f"Starting iteration {iteration_index}...")

        _run_iteration(
            workspace=workspace,
            worker_config_path=worker_config_path,
            worker_state_path=worker_state_path,
        )

        logging.info(f"Iteration {iteration_index} done. Will sleep a bit.")
        time.sleep(SECONDS_SLEEP_BETWEEN_ETL_ITERATIONS)


def _run_iteration(
    workspace: Path,
    worker_config_path: Path,
    worker_state_path: Path,
):
    worker_config = WorkerConfig.load_from_file(worker_config_path)
    worker_state = WorkerState.load_from_file(worker_state_path)

    now = int(_get_now().timestamp())
    max_initial_end_time = now - SECONDS_MIN_DELTA_BETWEEN_NOW_AND_ETL_ITERATION
    initial_end_timestamp = min(worker_config.time_partition_end, max_initial_end_time)

    if worker_state.latest_checkpoint_timestamp > max_initial_end_time:
        logging.info(f"Latest checkpoint is too recent. {worker_state.latest_checkpoint_timestamp - max_initial_end_time} seconds left before we can retry.")
        return

    worker_id = socket.gethostname()

    bq_client = _create_bq_client(worker_config.gcp_project_id)
    indexer = Indexer(worker_config.indexer_url)
    cloud_logger = CloudLogger(worker_config.gcp_project_id, worker_id)
    tasks_dashboard = TasksDashboard()
    file_storage = FileStorage(workspace)
    tasks_runner = TasksRunner(
        bq_client=bq_client,
        bq_dataset=worker_config.bq_dataset,
        indexer=indexer,
        file_storage=file_storage,
        schema_folder=worker_config.schema_folder
    )

    for bulk_index in range(0, sys.maxsize):
        cloud_logger.log_info(f"Starting bulk #{bulk_index}...")
        cloud_logger.log_info(f"Latest checkpoint: {worker_state.get_latest_checkpoint_datetime()}.")

        latest_planned_interval_end_time = tasks_dashboard.plan_bulk_with_intervals(
            indices=worker_config.indices_with_intervals,
            initial_start_timestamp=(worker_state.latest_checkpoint_timestamp or worker_config.time_partition_start),
            initial_end_timestamp=initial_end_timestamp,
            num_intervals_in_bulk=worker_config.num_intervals_in_bulk,
            interval_size_in_seconds=worker_config.interval_size_in_seconds
        )

        if latest_planned_interval_end_time is None:
            logging.warning("No tasks planned, nothing to do.")
            return

        _consume_tasks_in_parallel(
            dashboard=tasks_dashboard,
            runner=tasks_runner,
            num_threads=worker_config.num_threads,
        )

        failed_tasks = tasks_dashboard.get_failed_tasks()
        if failed_tasks:
            for task in failed_tasks:
                cloud_logger.log_error(f"Task has failed: {task.error}", task)

            logging.error(f"{len(failed_tasks)} tasks have failed, will stop.")
            break

        tasks_dashboard.assert_all_existing_tasks_are_finished()

        check_loaded_data(
            bq_client=bq_client,
            bq_dataset=worker_config.bq_dataset,
            indexer=indexer,
            tables=worker_config.indices_with_intervals,
            start_timestamp=worker_config.time_partition_start,
            end_timestamp=latest_planned_interval_end_time
        )

        worker_state.latest_checkpoint_timestamp = latest_planned_interval_end_time
        worker_state.save_to_file(worker_state_path)

        cloud_logger.log_info(f"Bulk #{bulk_index} done.")

    # TODO: Plan tasks without interval!


def _create_bq_client(gcp_project_id: str) -> bigquery.Client:
    bq_client = bigquery.Client(project=gcp_project_id)
    adapter = requests.adapters.HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=3)  # type: ignore
    bq_client._http.mount("https://", adapter)  # type: ignore
    bq_client._http._auth_request.session.mount("https://", adapter)  # type: ignore
    return bq_client


def _consume_tasks_in_parallel(
        dashboard: TasksDashboard,
        runner: TasksRunner,
        num_threads: int
):
    # If an error happens in any thread, we stop all threads.
    event_has_error_happened: threading.Event = threading.Event()
    threads: List[threading.Thread] = []

    for thread_index in range(num_threads):
        thread = threading.Thread(
            name=f"consume-task-{thread_index}",
            target=_consume_tasks_thread,
            args=[
                dashboard,
                runner,
                event_has_error_happened
            ]
        )

        thread.start()
        threads.append(thread)

    for thread in threads:
        if thread.is_alive():
            thread.join()


def _consume_tasks_thread(
    dashboard: TasksDashboard,
    runner: TasksRunner,
    external_or_internal_event_has_error_happened: threading.Event
):
    if external_or_internal_event_has_error_happened.is_set():
        return

    while True:
        task = dashboard.pick_and_start_task()
        if task is None:
            break

        try:
            runner.run(task)
            dashboard.on_task_finished(task)
        except Exception as error:
            logging.error(f"Error while consuming task {task}.")
            external_or_internal_event_has_error_happened.set()
            task.set_failed(error, traceback.format_exc())
            break


def _get_now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


if __name__ == "__main__":
    return_code = main(sys.argv[1:])
    sys.exit(return_code)
