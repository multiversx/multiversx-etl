
import datetime
import logging
import pprint
from io import TextIOWrapper
from pathlib import Path
from typing import List

from multiversxetl.planner.tasks import (
    Task, count_tasks_by_status, group_tasks_by_index_name,
    sort_tasks_within_groups_by_start_timestamp)


class TasksReporter:
    def __init__(self, output_folder: Path):
        self.output_folder = output_folder

    def generate_report(
        self,
        name: str,
        tasks_with_intervals: List[Task],
        tasks_without_intervals: List[Task],
        with_summary: bool = True,
        with_details: bool = True,
    ):
        logging.info(f"Generate report for {name}...")

        self.output_folder.mkdir(parents=True, exist_ok=True)

        if with_summary:
            with open(self.output_folder / f"report_tasks_with_intervals_summary_{name}.txt", "w") as file:
                self._report_tasks_summary(file, tasks_with_intervals)

            with open(self.output_folder / f"report_tasks_without_intervals_summary_{name}.txt", "w") as file:
                self._report_tasks_summary(file, tasks_without_intervals)

        if with_details:
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
        sort_tasks_within_groups_by_start_timestamp(tasks_by_index_name)

        for tasks in tasks_by_index_name.values():
            for task in tasks:
                loading_status = task.loading_status.value
                extraction_status = task.extraction_status.value

                if task.is_time_bound():
                    assert task.start_timestamp is not None
                    assert task.end_timestamp is not None
                    start = datetime.datetime.utcfromtimestamp(task.start_timestamp)
                    end = datetime.datetime.utcfromtimestamp(task.end_timestamp)
                    line = f"ID = {task.id}, index = {task.index_name}, start = {start}, end = {end}, interval = {task.get_interval_duration()}, status = [{loading_status}, {extraction_status}]"
                else:
                    line = f"ID = {task.id}, index = {task.index_name}, status = [{loading_status}, {extraction_status}]"
                print(line, file=file)
