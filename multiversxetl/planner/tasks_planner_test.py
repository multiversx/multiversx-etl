from multiversxetl.constants import (INDICES_WITH_INTERVALS,
                                     INDICES_WITHOUT_INTERVALS, SECONDS_IN_DAY)
from multiversxetl.planner.tasks import TaskStatus
from multiversxetl.planner.tasks_planner import TasksPlanner


def test_plan_tasks_with_intervals_with_trivial_intervals():
    planner = TasksPlanner()
    tasks = planner.plan_tasks_with_intervals("https://example.com", "foobar", 0, 1, 1)

    assert len(tasks) == len(INDICES_WITH_INTERVALS)
    assert all([task.indexer_url == "https://example.com" for task in tasks])
    assert all([task.bq_dataset == "foobar" for task in tasks])
    assert all([task.start_timestamp == 0 for task in tasks])
    assert all([task.end_timestamp == 1 for task in tasks])
    assert all([task.extraction_status == TaskStatus.PENDING for task in tasks])
    assert all([task.loading_status == TaskStatus.PENDING for task in tasks])


def test_plan_tasks_with_intervals_with_day_intervals():
    planner = TasksPlanner()

    start_time = 1596117600
    end_time = 1687508546
    num_expected_intervals = int((end_time - start_time) / SECONDS_IN_DAY) + 1
    tasks = planner.plan_tasks_with_intervals("https://example.com", "foobar", start_time, end_time, SECONDS_IN_DAY)

    for index_name in INDICES_WITH_INTERVALS:
        tasks_of_index = [task for task in tasks if task.index_name == index_name]

        assert len(tasks_of_index) == num_expected_intervals
        assert tasks_of_index[0].start_timestamp == start_time
        assert tasks_of_index[-1].end_timestamp == end_time

        for i in range(len(tasks_of_index) - 1):
            assert tasks_of_index[i].end_timestamp == tasks_of_index[i + 1].start_timestamp


def test_plan_tasks_with_intervals_without_intervals():
    planner = TasksPlanner()
    tasks = planner.plan_tasks_without_intervals("https://example.com", "foobar")

    assert len(tasks) == len(INDICES_WITHOUT_INTERVALS)
    assert all([task.indexer_url == "https://example.com" for task in tasks])
    assert all([task.bq_dataset == "foobar" for task in tasks])
    assert all([task.start_timestamp == None for task in tasks])
    assert all([task.end_timestamp == None for task in tasks])
    assert all([task.extraction_status == TaskStatus.PENDING for task in tasks])
    assert all([task.loading_status == TaskStatus.PENDING for task in tasks])
