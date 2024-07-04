import datetime
import logging
from typing import Any, List, Optional, Protocol

from google.cloud import bigquery

from multiversxetl.errors import CountsMismatchError
from multiversxetl.worker_config import CountChecksErratum


class IIndexer(Protocol):
    def count_records(self, index_name: str, start_timestamp: int, end_timestamp: int) -> int: ...


class IBqClient(Protocol):
    def run_query(self, query_parameters: List[bigquery.ScalarQueryParameter], query: str, into_table: Optional[str] = None) -> List[Any]: ...
    def get_num_records(self, bq_dataset: str, table_name: str) -> int: ...
    def get_num_records_in_interval(self, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> int: ...


def check_loaded_data(
    bq_client: IBqClient,
    bq_dataset: str,
    indexer: IIndexer,
    tables: List[str],
    start_timestamp: int,
    end_timestamp: int,
    use_global_counts_for_bq: bool,
    should_fail_on_counts_mismatch: bool,
    skip_counts_check_for_indices: List[str],
    counts_checks_erratum: CountChecksErratum
):
    for table in tables:
        if table in skip_counts_check_for_indices:
            continue

        _do_check_loaded_data_for_table(
            bq_client,
            bq_dataset,
            indexer,
            table,
            start_timestamp,
            end_timestamp,
            use_global_counts_for_bq,
            should_fail_on_counts_mismatch,
            counts_checks_erratum
        )


def _do_check_loaded_data_for_table(
        bq_client: IBqClient,
        bq_dataset: str,
        indexer: IIndexer,
        table: str,
        start_timestamp: int,
        end_timestamp: int,
        use_global_counts_for_bq: bool,
        should_fail_on_counts_mismatch: bool,
        counts_checks_erratum: CountChecksErratum
):
    start_datetime = datetime.datetime.fromtimestamp(start_timestamp, tz=datetime.timezone.utc)
    end_datetime = datetime.datetime.fromtimestamp(end_timestamp, tz=datetime.timezone.utc)
    logging.info(f"Checking table = {table}, start = {start_timestamp} ({start_datetime}), end = {end_timestamp} ({end_datetime})")

    count_in_indexer = indexer.count_records(table, start_timestamp, end_timestamp)

    if use_global_counts_for_bq:
        count_in_bq = bq_client.get_num_records(bq_dataset, table)
    else:
        count_in_bq = bq_client.get_num_records_in_interval(bq_dataset, table, start_timestamp, end_timestamp)

    counts_delta = count_in_indexer - count_in_bq

    if counts_delta == 0:
        logging.info(f"Counts match for '{table}': {count_in_bq}.")
    else:
        logging.warning(f"Counts do not match for '{table}': indexer = {count_in_indexer}, bq = {count_in_bq}, delta = {counts_delta}.")

    if not should_fail_on_counts_mismatch:
        return

    if counts_delta > 0:
        raise CountsMismatchError(f"Data is missing in BigQuery for table '{table}'. Delta = {counts_delta}.")

    if counts_delta < 0:
        # We do not automatically perform de-duplication, because the operation is quite expensive.
        # Instead, we stop the flow. At restart, duplicated records would be removed (due to the rewind step).
        raise CountsMismatchError(f"Counts do not match, there may be duplicated data in BigQuery, table '{table}': indexer = {count_in_indexer}, bq = {count_in_bq}, delta = {counts_delta}.")
