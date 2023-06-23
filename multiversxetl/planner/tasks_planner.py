import uuid
from typing import List

from multiversxetl.constants import (INDEXES_WITH_INTERVALS,
                                     INDEXES_WITHOUT_INTERVALS)
from multiversxetl.planner.tasks import Task


class TasksPlanner:
    def __init__(self):
        pass

    def plan_tasks_with_intervals(
            self,
            indexer_url: str,
            bq_dataset: str,
            initial_start_timestamp: int,
            initial_end_timestamp: int,
            granularity_seconds: int
    ) -> List[Task]:
        tasks: List[Task] = []

        for index_name in INDEXES_WITH_INTERVALS:
            for start_timestamp in range(initial_start_timestamp, initial_end_timestamp, granularity_seconds):
                id = self._next_task_id()
                end_timestamp = min(start_timestamp + granularity_seconds, initial_end_timestamp)
                task = Task(id, indexer_url, index_name, bq_dataset, start_timestamp, end_timestamp)
                tasks.append(task)

        return tasks

    def plan_tasks_without_intervals(self, indexer_url: str, bq_dataset: str) -> List[Task]:
        tasks: List[Task] = []

        for index_name in INDEXES_WITHOUT_INTERVALS:
            id = self._next_task_id()
            task = Task(id, indexer_url, index_name, bq_dataset)
            tasks.append(task)

        return tasks

    def _next_task_id(self, ) -> str:
        return uuid.uuid4().hex
