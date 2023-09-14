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
from typing import List, Optional

import requests
from google.cloud import bigquery

from multiversxetl.checks import check_loaded_data
from multiversxetl.constants import (
    SECONDS_MIN_DELTA_BETWEEN_NOW_AND_APPEND_ONLY_INDICES_EXTRACTION_END_TIME,
    SECONDS_SLEEP_BETWEEN_ETL_ITERATIONS)
from multiversxetl.errors import UsageError
from multiversxetl.file_storage import FileStorage
from multiversxetl.indexer import Indexer
from multiversxetl.logger import CloudLogger
from multiversxetl.tasks_dashboard import TasksDashboard
from multiversxetl.tasks_runner import TasksRunner
from multiversxetl.worker_config import IndicesConfig, WorkerConfig
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
    append_only_indices_config = worker_config.append_only_indices
    mutable_indices_config = worker_config.mutable_indices
    worker_state = WorkerState.load_from_file(worker_state_path)

    iteration_start_time = int(_get_now().timestamp())
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

    # This will be the end time for both mutable and append-only indices.
    initial_end_timestamp = iteration_start_time

    # First, we truncate the mutable indices (they will be reloaded from scratch).
    _truncate_tables(
        bq_client=bq_client,
        bq_dataset=worker_config.bq_dataset,
        tables=mutable_indices_config.indices,
    )

    # At this time, "iteration_start_time" is very close to the current time (now).
    _ = _plan_and_consume_bulk(
        indexer=indexer,
        bq_client=bq_client,
        bq_dataset=worker_config.bq_dataset,
        tasks_dashboard=tasks_dashboard,
        tasks_runner=tasks_runner,
        cloud_logger=cloud_logger,
        indices_config=mutable_indices_config,
        initial_start_timestamp=worker_config.genesis_timestamp,
        initial_end_timestamp=initial_end_timestamp
    )

    # At this time, "iteration_start_time" is in the past (a few minutes, to tens of minutes).
    # Assertion required for the "append-only" assumption to hold (e.g. transactions are mutated shortly after being indexed).
    assert int(_get_now().timestamp()) - initial_end_timestamp > SECONDS_MIN_DELTA_BETWEEN_NOW_AND_APPEND_ONLY_INDICES_EXTRACTION_END_TIME

    for bulk_index in range(0, sys.maxsize):
        cloud_logger.log_info(f"Starting bulk #{bulk_index}...")
        cloud_logger.log_info(f"Latest checkpoint: {worker_state.get_latest_checkpoint_datetime()}.")

        latest_checkpoint_timestamp = _plan_and_consume_bulk(
            indexer=indexer,
            bq_client=bq_client,
            bq_dataset=worker_config.bq_dataset,
            tasks_dashboard=tasks_dashboard,
            tasks_runner=tasks_runner,
            cloud_logger=cloud_logger,
            indices_config=append_only_indices_config,
            initial_start_timestamp=(worker_state.latest_checkpoint_timestamp or append_only_indices_config.time_partition_start),
            # Best-effort consistency across mutable and append-only indices.
            initial_end_timestamp=initial_end_timestamp
        )

        if latest_checkpoint_timestamp is None:
            return

        worker_state.latest_checkpoint_timestamp = latest_checkpoint_timestamp
        worker_state.save_to_file(worker_state_path)

        cloud_logger.log_info(f"Bulk #{bulk_index} done.")


def _create_bq_client(gcp_project_id: str) -> bigquery.Client:
    bq_client = bigquery.Client(project=gcp_project_id)
    adapter = requests.adapters.HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=3)  # type: ignore
    bq_client._http.mount("https://", adapter)  # type: ignore
    bq_client._http._auth_request.session.mount("https://", adapter)  # type: ignore
    return bq_client


def _plan_and_consume_bulk(
    indexer: Indexer,
    bq_client: bigquery.Client,
    bq_dataset: str,
    tasks_dashboard: TasksDashboard,
    tasks_runner: TasksRunner,
    cloud_logger: CloudLogger,
    indices_config: IndicesConfig,
    initial_start_timestamp: int,
    initial_end_timestamp: int
) -> Optional[int]:
    latest_planned_interval_end_time = tasks_dashboard.plan_bulk(
        indices=indices_config.indices,
        initial_start_timestamp=initial_start_timestamp,
        initial_end_timestamp=initial_end_timestamp,
        num_intervals_in_bulk=indices_config.num_intervals_in_bulk,
        interval_size_in_seconds=indices_config.interval_size_in_seconds
    )

    if latest_planned_interval_end_time is None:
        logging.warning("No tasks planned, nothing to do.")
        return

    _consume_tasks_in_parallel(
        tasks_dashboard=tasks_dashboard,
        tasks_runner=tasks_runner,
        num_threads=indices_config.num_threads,
    )

    failed_tasks = tasks_dashboard.get_failed_tasks()
    if failed_tasks:
        for task in failed_tasks:
            cloud_logger.log_error(f"Task has failed: {task.error}", task)

        logging.error(f"{len(failed_tasks)} tasks have failed, will stop.")
        return

    tasks_dashboard.assert_all_existing_tasks_are_finished()

    check_loaded_data(
        bq_client=bq_client,
        bq_dataset=bq_dataset,
        indexer=indexer,
        tables=indices_config.indices,
        start_timestamp=indices_config.time_partition_start,
        end_timestamp=latest_planned_interval_end_time,
        should_fail_on_counts_mismatch=indices_config.should_fail_on_counts_mismatch
    )

    return latest_planned_interval_end_time


def _consume_tasks_in_parallel(
        tasks_dashboard: TasksDashboard,
        tasks_runner: TasksRunner,
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
                tasks_dashboard,
                tasks_runner,
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
    while True:
        if external_or_internal_event_has_error_happened.is_set():
            break

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


def _truncate_tables(bq_client: bigquery.Client, bq_dataset: str, tables: List[str]) -> None:
    for table in tables:
        table_ref = bq_client.dataset(bq_dataset).table(table)
        bq_client.delete_table(table_ref)


if __name__ == "__main__":
    return_code = main(sys.argv[1:])
    sys.exit(return_code)
