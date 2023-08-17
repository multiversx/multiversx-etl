from multiversxetl.constants import SECONDS_IN_DAY
from multiversxetl.planner.tasks import TaskStatus
from multiversxetl.planner.tasks_planner import TasksPlanner


def test_plan_tasks_with_intervals_with_trivial_intervals():
    planner = TasksPlanner()
    tasks = planner.plan_tasks_with_intervals("https://example.com", "testIndex", "testDataset", 0, 1, 1)

    assert len(tasks) == 1
    assert all([task.indexer_url == "https://example.com" for task in tasks])
    assert all([task.index_name == "testIndex" for task in tasks])
    assert all([task.bq_dataset == "testDataset" for task in tasks])
    assert all([task.start_timestamp == 0 for task in tasks])
    assert all([task.end_timestamp == 1 for task in tasks])
    assert all([task.extraction_status == TaskStatus.PENDING for task in tasks])
    assert all([task.loading_status == TaskStatus.PENDING for task in tasks])


def test_plan_tasks_with_intervals_with_day_intervals():
    planner = TasksPlanner()

    start_time = 1596117600
    end_time = 1687508546
    num_expected_intervals = int((end_time - start_time) / SECONDS_IN_DAY) + 1
    tasks = planner.plan_tasks_with_intervals("https://example.com", "testIndex", "testDataset", start_time, end_time, SECONDS_IN_DAY)

    assert len(tasks) == num_expected_intervals
    assert tasks[0].start_timestamp == start_time
    assert tasks[-1].end_timestamp == end_time

    for i in range(len(tasks) - 1):
        assert tasks[i].index_name == "testIndex"
        assert tasks[i].bq_dataset == "testDataset"
        assert tasks[i].end_timestamp == tasks[i + 1].start_timestamp


def test_plan_tasks_with_intervals_without_intervals():
    planner = TasksPlanner()
    tasks = planner.plan_tasks_without_intervals("https://example.com", "testIndex", "testDataset")

    assert len(tasks) == 1
    assert all([task.indexer_url == "https://example.com" for task in tasks])
    assert all([task.index_name == "testIndex" for task in tasks])
    assert all([task.bq_dataset == "testDataset" for task in tasks])
    assert all([task.start_timestamp == None for task in tasks])
    assert all([task.end_timestamp == None for task in tasks])
    assert all([task.extraction_status == TaskStatus.PENDING for task in tasks])
    assert all([task.loading_status == TaskStatus.PENDING for task in tasks])
