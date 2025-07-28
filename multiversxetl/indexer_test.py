
import datetime
from typing import Tuple

import pytest

from multiversxetl.indexer import Indexer


@pytest.mark.integration
def test_count_records():
    start_timestamp, end_timestamp = _make_recent_time_slice(24 * 60 * 60)

    indexer = Indexer("https://index.multiversx.com")
    count = indexer.count_records("operations", start_timestamp, end_timestamp)
    assert count > 42

    indexer = Indexer("https://devnet-index.multiversx.com")
    count = indexer.count_records("operations", start_timestamp, end_timestamp)
    assert count > 0

    indexer = Indexer("https://testnet-index.multiversx.com")
    count = indexer.count_records("operations", start_timestamp, end_timestamp)
    assert count > 0


@pytest.mark.integration
def test_get_records():
    indexer = Indexer("https://index.multiversx.com")
    records = indexer.get_records("operations", *_make_recent_time_slice(60))
    assert any(records)

    indexer = Indexer("https://devnet-index.multiversx.com")
    records = indexer.get_records("operations", *_make_recent_time_slice(60 * 60))
    assert any(records)

    indexer = Indexer("https://testnet-index.multiversx.com")
    records = indexer.get_records("operations", *_make_recent_time_slice(24 * 60 * 60))
    assert any(records)


def _make_recent_time_slice(duration_in_seconds: int) -> Tuple[int, int]:
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    now_timestamp = int(now.timestamp())
    some_time_ago = (now - datetime.timedelta(seconds=duration_in_seconds))
    some_time_ago_timestamp = int(some_time_ago.timestamp())
    return some_time_ago_timestamp, now_timestamp
