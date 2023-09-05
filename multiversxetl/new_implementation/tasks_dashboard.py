import threading
from typing import List, Optional

from multiversxetl.new_implementation.task import (Task, TaskWithInterval,
                                                   TaskWithoutInterval)


class TasksDashboard:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._tasks: List[Task] = []

    def plan_bulk_with_intervals(
            self,
            indices: List[str],
            initial_start_timestamp: int,
            initial_end_timestamp: int,
            num_intervals_in_bulk: int,
            interval_size_in_seconds: int,
    ) -> Optional[int]:
        """
        This should not be called concurrently with other methods.

        Returns the end time of the latest interval for the planned tasks.
        """
        self._assert_all_existing_tasks_are_finished()

        end_timestamp_of_latest_interval: Optional[int] = None

        for interval_index in range(num_intervals_in_bulk):
            start_timestamp = initial_start_timestamp + interval_index * interval_size_in_seconds
            end_timestamp = min(start_timestamp + interval_size_in_seconds, initial_end_timestamp)

            if end_timestamp > initial_end_timestamp:
                break

            end_timestamp_of_latest_interval = end_timestamp

            for index_name in indices:
                task = TaskWithInterval(index_name, start_timestamp, end_timestamp)
                self._tasks.append(task)

        return end_timestamp_of_latest_interval

    def plan_bulk_without_intervals(self, indices: List[str]) -> None:
        """
        This should not be called concurrently with other methods.
        """
        self._assert_all_existing_tasks_are_finished()

        for index_name in indices:
            task = TaskWithoutInterval(index_name)
            self._tasks.append(task)

    def pick_next_task(self) -> Optional[Task]:
        """
        This can be called concurrently with "pick_next_task" or "on_task_finished".
        """
        with self._lock:
            for task in self._tasks:
                if task.is_pending():
                    task.set_started()
                    return task

        return None

    def on_task_finished(self, task: Task) -> None:
        """
        This can be called concurrently with "pick_next_task" or "on_task_finished".
        """
        with self._lock:
            assert task.is_started(), f"Task {task} must have had the status 'started'."
            assert task in self._tasks, f"Task {task} is not in the list of tasks."
            task.set_finished()

    def _assert_all_existing_tasks_are_finished(self) -> None:
        for task in self._tasks:
            assert task.is_finished(), f"Task {task} is not finished."
