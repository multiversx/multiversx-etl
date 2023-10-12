import datetime
import logging
import socket
import sys
import threading
import traceback
from pathlib import Path
from typing import List, Optional

from multiversxetl.bq_client import BqClient
from multiversxetl.checks import check_loaded_data
from multiversxetl.constants import END_TIME_DELTA
from multiversxetl.errors import SomeTasksFailedError, UsageError
from multiversxetl.file_storage import FileStorage
from multiversxetl.indexer import Indexer
from multiversxetl.logger import CloudLogger
from multiversxetl.tasks_dashboard import TasksDashboard
from multiversxetl.tasks_runner import TasksRunner
from multiversxetl.worker_config import IndicesConfig, WorkerConfig
from multiversxetl.worker_state import WorkerState


class AppController:
    def __init__(self, workspace: Path) -> None:
        worker_config_path = workspace / "worker_config.json"
        self.worker_state_path = workspace / "worker_state.json"

        if not worker_config_path.exists():
            raise UsageError(f"Worker config file not found: {worker_config_path}")
        if not self.worker_state_path.exists():
            raise UsageError(f"Worker state file not found: {self.worker_state_path}")

        self.worker_config = WorkerConfig.load_from_file(worker_config_path)
        self.worker_state = WorkerState.load_from_file(self.worker_state_path)
        worker_id = socket.gethostname()

        self.bq_client = BqClient(self.worker_config.gcp_project_id)
        self.indexer = Indexer(self.worker_config.indexer_url)
        self.cloud_logger = CloudLogger(self.worker_config.gcp_project_id, worker_id)
        self.tasks_dashboard = TasksDashboard()
        file_storage = FileStorage(workspace)
        self.tasks_runner = TasksRunner(
            bq_client=self.bq_client,
            indexer=self.indexer,
            file_storage=file_storage,
            schema_folder=self.worker_config.schema_folder
        )

    def process_mutable_indices(self):
        indices_config = self.worker_config.mutable_indices

        now = int(_get_now().timestamp())

        # First, we truncate the mutable indices (they will be reloaded from scratch).
        self.bq_client.truncate_tables(
            bq_dataset=indices_config.bq_dataset,
            tables=indices_config.indices + indices_config.indices_without_timestamp,
        )

        _ = self._plan_and_consume_bulk(
            indices_config=indices_config,
            initial_start_timestamp=self.worker_config.genesis_timestamp,
            initial_end_timestamp=now
        )

    def process_append_only_indices(self):
        indices_config = self.worker_config.append_only_indices

        now = int(_get_now().timestamp())
        max_initial_end_timestamp = now - END_TIME_DELTA

        initial_end_timestamp = min(
            max_initial_end_timestamp,
            indices_config.time_partition_end
        ) if indices_config.time_partition_end > 0 else max_initial_end_timestamp

        for bulk_index in range(0, sys.maxsize):
            self.cloud_logger.log_info(f"Starting bulk #{bulk_index}...")
            self.cloud_logger.log_info(f"Latest checkpoint: {self.worker_state.get_latest_checkpoint_datetime()}.")

            latest_checkpoint_timestamp = self._plan_and_consume_bulk(
                indices_config=indices_config,
                initial_start_timestamp=(self.worker_state.latest_checkpoint_timestamp or indices_config.time_partition_start),
                initial_end_timestamp=initial_end_timestamp
            )

            if latest_checkpoint_timestamp is None:
                return

            self.worker_state.latest_checkpoint_timestamp = latest_checkpoint_timestamp
            self.worker_state.save_to_file(self.worker_state_path)

            self.cloud_logger.log_info(f"Bulk #{bulk_index} done.")

    def _plan_and_consume_bulk(
        self,
        indices_config: IndicesConfig,
        initial_start_timestamp: int,
        initial_end_timestamp: int
    ) -> Optional[int]:
        latest_planned_interval_end_time = self.tasks_dashboard.plan_bulk(
            bq_dataset=indices_config.bq_dataset,
            indices=indices_config.indices,
            indices_without_timestamp=indices_config.indices_without_timestamp,
            initial_start_timestamp=initial_start_timestamp,
            initial_end_timestamp=initial_end_timestamp,
            num_intervals_in_bulk=indices_config.num_intervals_in_bulk,
            interval_size_in_seconds=indices_config.interval_size_in_seconds
        )

        if latest_planned_interval_end_time is None:
            logging.warning("No tasks planned, nothing to do.")
            return

        self.tasks_dashboard.report_tasks()

        self._consume_tasks_in_parallel(
            num_threads=indices_config.num_threads,
        )

        failed_tasks = self.tasks_dashboard.get_failed_tasks()
        if failed_tasks:
            for task in failed_tasks:
                self.cloud_logger.log_error(f"Task has failed: {task.error}", task)

            logging.error(f"{len(failed_tasks)} tasks have failed, will stop.")
            raise SomeTasksFailedError()

        self.tasks_dashboard.assert_all_existing_tasks_are_finished()

        check_loaded_data(
            bq_client=self.bq_client,
            bq_dataset=indices_config.bq_dataset,
            indexer=self.indexer,
            tables=indices_config.indices,
            start_timestamp=indices_config.time_partition_start,
            end_timestamp=latest_planned_interval_end_time,
            should_fail_on_counts_mismatch=indices_config.should_fail_on_counts_mismatch,
            skip_counts_check_for_indices=indices_config.skip_counts_check_for_indices
        )

        return latest_planned_interval_end_time

    def _consume_tasks_in_parallel(self, num_threads: int):
        # If an error happens in any thread, we stop all threads.
        event_has_encountered_an_error: threading.Event = threading.Event()
        threads: List[threading.Thread] = []

        for thread_index in range(num_threads):
            thread = threading.Thread(
                name=f"consume-task-{thread_index}",
                target=self._consume_tasks_thread,
                args=[
                    event_has_encountered_an_error
                ]
            )

            thread.start()
            threads.append(thread)

        for thread in threads:
            if thread.is_alive():
                thread.join()

    def _consume_tasks_thread(self, external_or_internal_event_has_encountered_an_error: threading.Event):
        while True:
            if external_or_internal_event_has_encountered_an_error.is_set():
                break

            task = self.tasks_dashboard.pick_and_start_task()
            if task is None:
                break

            try:
                self.tasks_runner.run(task)
                self.tasks_dashboard.on_task_finished(task)
            except Exception as error:
                logging.error(f"Error while consuming task {task}.")
                external_or_internal_event_has_encountered_an_error.set()
                task.set_failed(error, traceback.format_exc())
                break

    def rewind_to_checkpoint(self):
        """
        From the BQ tables corresponding to append-only indices, deletes records newer than the latest checkpoint.
        """
        bq_dataset = self.worker_config.append_only_indices.bq_dataset
        indices = self.worker_config.append_only_indices.indices
        checkpoint_timestamp = self.worker_state.latest_checkpoint_timestamp

        logging.info(f"Rewinding to checkpoint {checkpoint_timestamp}...")

        for table in indices:
            self.bq_client.delete_newer_than(bq_dataset, table, checkpoint_timestamp)

        check_loaded_data(
            bq_client=self.bq_client,
            bq_dataset=bq_dataset,
            indexer=self.indexer,
            tables=indices,
            start_timestamp=self.worker_config.append_only_indices.time_partition_start,
            end_timestamp=checkpoint_timestamp,
            should_fail_on_counts_mismatch=True,
            skip_counts_check_for_indices=[]
        )


def _get_now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)
