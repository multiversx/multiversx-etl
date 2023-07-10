import datetime
from typing import Any

import pytest

from multiversxetl.cli.plan_tasks import \
    validate_and_coalesce_end_timestamp_for_plan_tasks_with_intervals
from multiversxetl.constants import \
    MIN_DISTANCE_FROM_CURRENT_TIME_FOR_EXTRACTION


def test_validate_and_coalesce_end_timestamp_for_plan_tasks_with_intervals(monkeypatch: Any):
    now = datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    max_time = int(now.timestamp()) - MIN_DISTANCE_FROM_CURRENT_TIME_FOR_EXTRACTION
    assert max_time == 1672529400

    class DummyDatetime:
        @classmethod
        def utcnow(cls):
            return now

    monkeypatch.setattr(datetime, "datetime", DummyDatetime)

    # No time specified
    timestamp = validate_and_coalesce_end_timestamp_for_plan_tasks_with_intervals(0)
    assert timestamp == max_time

    # Good time specified
    timestamp = validate_and_coalesce_end_timestamp_for_plan_tasks_with_intervals(max_time - 1)
    assert timestamp == max_time - 1

    # Bad time specified (too recent)
    with pytest.raises(Exception, match=f"End timestamp {max_time+1} is too recent. It should be at most {max_time} \\(current time - {MIN_DISTANCE_FROM_CURRENT_TIME_FOR_EXTRACTION} seconds\\)."):
        validate_and_coalesce_end_timestamp_for_plan_tasks_with_intervals(max_time + 1)
