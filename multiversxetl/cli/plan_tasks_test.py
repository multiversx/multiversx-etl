import datetime
from typing import Any

import pytest

from multiversxetl.cli.plan_tasks import decide_end_timestamp
from multiversxetl.constants import MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION


def test_decide_end_timestamp(monkeypatch: Any):
    now = datetime.datetime(2023, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
    max_time = int(now.timestamp()) - MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION
    assert max_time == 1672529400

    class DummyDatetime:
        @classmethod
        def utcnow(cls):
            return now

    monkeypatch.setattr(datetime, "datetime", DummyDatetime)

    # No time specified
    timestamp = decide_end_timestamp(0)
    assert timestamp == max_time

    # Good time specified
    timestamp = decide_end_timestamp(max_time - 1)
    assert timestamp == max_time - 1

    # Bad time specified (too recent)
    with pytest.raises(Exception, match=f"End timestamp {max_time+1} is too recent. It should be at most {max_time} \\(current time - {MIN_TIME_DELTA_FROM_NOW_FOR_EXTRACTION} seconds\\)."):
        decide_end_timestamp(max_time + 1)
