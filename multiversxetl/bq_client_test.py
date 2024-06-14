
from pathlib import Path

import pytest

from multiversxetl.bq_client import BqClient

testdata = Path(__file__).parent / "testdata"


@pytest.mark.integration
def test_delete_on_or_after_timestamp():
    timestamp = 1704060000
    timestamp_infinity = 32503672800

    client = BqClient("multiversx-blockchain-etl")
    client.load_data("tests", "bq_client_test", testdata / "dummy_schema.json", testdata / "dummy_data.txt")
    num_records = client.get_num_records_in_interval("tests", "bq_client_test", timestamp, timestamp_infinity)
    assert num_records > 0

    client.delete_on_or_after_timestamp("tests", "bq_client_test", timestamp)
    num_records = client.get_num_records_in_interval("tests", "bq_client_test", timestamp, timestamp_infinity)
    assert num_records == 0
