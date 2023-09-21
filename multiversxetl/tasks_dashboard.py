import datetime
import logging
import random
import threading
from typing import List, Optional

from multiversxetl.task import Task


class TasksDashboard:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: List[Task] = []

    def plan_bulk(
            self,
            bq_dataset: str,
            indices: List[str],
            indices_without_timestamp: List[str],
            initial_start_timestamp: int,
            initial_end_timestamp: int,
            num_intervals_in_bulk: int,
            interval_size_in_seconds: int,
    ) -> Optional[int]:
        """
        This should not be called concurrently with other methods.

        Returns the end time of the latest interval for the planned tasks.
        """
        self.assert_all_existing_tasks_are_finished()
        self._clear_all_existing_tasks()

        end_timestamp_of_latest_interval: Optional[int] = None

        for interval_index in range(num_intervals_in_bulk):
            start_timestamp = initial_start_timestamp + interval_index * interval_size_in_seconds
            end_timestamp = min(start_timestamp + interval_size_in_seconds, initial_end_timestamp)

            if start_timestamp >= initial_end_timestamp:
                break

            end_timestamp_of_latest_interval = end_timestamp

            for index_name in set(indices) - set(indices_without_timestamp):
                task = Task(bq_dataset, index_name, start_timestamp, end_timestamp)
                self._tasks.append(task)

        for index_name in indices_without_timestamp:
            task = Task(bq_dataset, index_name)
            self._tasks.append(task)

        # Consumers will randomly pick tasks.
        self._shuffle_all_existing_tasks()

        return end_timestamp_of_latest_interval

    def pick_and_start_task(self) -> Optional[Task]:
        """
        This can be called concurrently with "pick_next_task" or "on_task_finished".
        """
        with self._lock:
            self._report_tasks_status("pick_and_start_task()")

            for task in self._tasks:
                if task.is_pending():
                    task.set_started(self._get_now())
                    return task

    def on_task_finished(self, task: Task) -> None:
        task.set_finished(self._get_now())
        logging.info(f"Task {task} finished. Took {task.get_duration()} seconds.")
        self._report_tasks_status("on_task_finished()")
        pass

    def assert_all_existing_tasks_are_finished(self) -> None:
        """
        This should not be called concurrently with other methods.
        """
        for task in self._tasks:
            assert task.is_finished(), f"Task {task} is not finished."

    def _clear_all_existing_tasks(self) -> None:
        self._tasks.clear()

    def _shuffle_all_existing_tasks(self) -> None:
        random.shuffle(self._tasks)

    def _report_tasks_status(self, message: str) -> None:
        num_pending = len([task for task in self._tasks if task.is_pending()])
        num_started = len([task for task in self._tasks if task.is_started()])
        num_finished = len([task for task in self._tasks if task.is_finished()])
        num_failed = len([task for task in self._tasks if task.is_failed()])

        logging.info(f"{message}: pending = {num_pending}, started = {num_started}, finished = {num_finished}, failed = {num_failed}, total = {len(self._tasks)}.")

    def get_failed_tasks(self) -> List[Task]:
        """
        This should not be called concurrently with other methods.
        """
        return [task for task in self._tasks if task.is_failed()]

    def _get_now(self) -> datetime.datetime:
        return datetime.datetime.now(tz=datetime.timezone.utc)
