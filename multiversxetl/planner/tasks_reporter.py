
import datetime
import pprint
from io import TextIOWrapper
from pathlib import Path
from typing import List

from multiversxetl.planner.tasks import (Task, count_tasks_by_status,
                                         group_tasks_by_index_name)


class TasksReporter:
    def __init__(self, output_folder: Path):
        self.output_folder = output_folder

    def generate_report(self, name: str, tasks_with_intervals: List[Task], tasks_without_intervals: List[Task]):
        self.output_folder.mkdir(parents=True, exist_ok=True)

        with open(self.output_folder / f"report_tasks_with_intervals_summary_{name}.txt", "w") as file:
            self._report_tasks_summary(file, tasks_with_intervals)

        with open(self.output_folder / f"report_tasks_without_intervals_summary_{name}.txt", "w") as file:
            self._report_tasks_summary(file, tasks_without_intervals)

        with open(self.output_folder / f"report_tasks_with_intervals_{name}.txt", "w") as file:
            self._report_tasks_details(file, tasks_with_intervals)

        with open(self.output_folder / f"report_tasks_without_intervals_{name}.txt", "w") as file:
            self._report_tasks_details(file, tasks_without_intervals)

    def _report_tasks_summary(self, file: TextIOWrapper, tasks: List[Task]):
        counts_by_extraction_status, counts_by_loading_status = count_tasks_by_status(tasks)

        print("By extraction status:", file=file)
        pprint.pprint(counts_by_extraction_status, indent=4, stream=file)

        print("By loading status:", file=file)
        pprint.pprint(counts_by_loading_status, indent=4, stream=file)

    def _report_tasks_details(self, file: TextIOWrapper, tasks: List[Task]):
        tasks_by_index_name = group_tasks_by_index_name(tasks)

        for tasks in tasks_by_index_name.values():
            for task in tasks:
                start = datetime.datetime.utcfromtimestamp(task.start_timestamp) if task.start_timestamp else None
                end = datetime.datetime.utcfromtimestamp(task.end_timestamp) if task.end_timestamp else None
                loading_status = task.loading_status.value
                extraction_status = task.extraction_status.value

                line = f"ID = {task.id}, index = {task.index_name}, start = {start}, end = {end}, status = [{loading_status}, {extraction_status}]"
                print(line, file=file)
