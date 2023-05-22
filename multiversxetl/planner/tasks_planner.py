import uuid
from typing import List

from multiversxetl.planner.tasks import Task

INDEXES_WITH_INTERVALS = ["accountsesdt", "tokens", "blocks", "receipts", "transactions", "miniblocks", "rounds", "accountshistory", "scresults", "accountsesdthistory", "scdeploys", "logs", "operations"]
INDEXES_WITHOUT_INTERVALS = set(["accounts", "rating", "validators", "epochinfo", "tags", "delegators"]) - set(["rating", "validators", "tags"])


class TasksPlanner:
    def __init__(self):
        pass

    def plan_tasks_with_intervals(
            self,
            indexer_url: str,
            bq_dataset_fqn: str,
            initial_start_timestamp: int,
            initial_end_timestamp: int,
            granularity_seconds: int
    ) -> List[Task]:
        tasks: List[Task] = []

        for index_name in INDEXES_WITH_INTERVALS:
            for start_timestamp in range(initial_start_timestamp, initial_end_timestamp, granularity_seconds):
                id = self._next_task_id()
                end_timestamp = start_timestamp + granularity_seconds
                task = Task(id, indexer_url, index_name, bq_dataset_fqn, start_timestamp, end_timestamp)
                tasks.append(task)

        return tasks

    def plan_tasks_without_intervals(self, indexer_url: str, bq_dataset_fqn: str) -> List[Task]:
        tasks: List[Task] = []

        for index_name in INDEXES_WITHOUT_INTERVALS:
            id = self._next_task_id()
            task = Task(id, indexer_url, index_name, bq_dataset_fqn)
            tasks.append(task)

        return tasks

    def _next_task_id(self, ) -> str:
        return uuid.uuid4().hex
