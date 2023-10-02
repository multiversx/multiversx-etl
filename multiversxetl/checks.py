import datetime
import logging
from typing import Any, List, Optional, Protocol

from google.cloud import bigquery


class IIndexer(Protocol):
    def count_records(self, index_name: str, start_timestamp: int, end_timestamp: int) -> int: ...


class IBqClient(Protocol):
    def run_query(self, query_parameters: List[bigquery.ScalarQueryParameter], query: str, into_table: Optional[str] = None) -> List[Any]: ...


def check_loaded_data(
    bq_client: IBqClient,
    bq_dataset: str,
    indexer: IIndexer,
    tables: List[str],
    start_timestamp: int,
    end_timestamp: int,
    should_fail_on_counts_mismatch: bool,
    skip_counts_check_for_indices: List[str]
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
            should_fail_on_counts_mismatch
        )


def _do_check_loaded_data_for_table(
        bq_client: IBqClient,
        bq_dataset: str,
        indexer: IIndexer,
        table: str,
        start_timestamp: int,
        end_timestamp: int,
        should_fail_on_counts_mismatch: bool
):
    start_datetime = datetime.datetime.fromtimestamp(start_timestamp, tz=datetime.timezone.utc)
    end_datetime = datetime.datetime.fromtimestamp(end_timestamp, tz=datetime.timezone.utc)
    logging.info(f"Checking table = {table}, start = {start_timestamp} ({start_datetime}), end = {end_timestamp} ({end_datetime})")

    count_in_indexer = indexer.count_records(table, start_timestamp, end_timestamp)
    count_in_bq = _get_num_records_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)
    counts_delta = count_in_indexer - count_in_bq

    if counts_delta == 0:
        logging.info(f"Counts match for '{table}'.")
    else:
        logging.warning(f"Counts do not match for '{table}': indexer = {count_in_indexer}, bq = {count_in_bq}, delta = {counts_delta}.")

    if not should_fail_on_counts_mismatch:
        return

    if counts_delta > 0:
        raise Exception(f"Data is missing in BigQuery for table '{table}'. Delta = {counts_delta}.")

    if counts_delta < 0:
        logging.warning(f"More records in BigQuery than in indexer. Will attempt to de-duplicate.")

        # We do not perform a duplication check (most probably a positive anyway), we directly de-duplicate, to save costs.
        _deduplicate_table(bq_client, bq_dataset, table)

        # Check counts again.
        count_in_bq = _get_num_records_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)
        counts_delta = count_in_indexer - count_in_bq

        if counts_delta != 0:
            raise Exception(f"Counts do not match after deduplication: indexer = {count_in_indexer}, bq = {count_in_bq}, delta = {counts_delta}.")


def _get_num_records_in_interval(bq_client: IBqClient, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> int:
    query = _create_query_for_get_num_records_in_interval(bq_dataset, table)
    query_parameters = _create_query_parameters_for_interval(start_timestamp, end_timestamp)
    records = bq_client.run_query(query_parameters, query)
    return records[0].count


def _create_query_for_get_num_records_in_interval(dataset: str, table: str):
    return f"""
    SELECT COUNT(*) AS `count`
    FROM `{dataset}.{table}`
    WHERE `timestamp` >= TIMESTAMP_SECONDS(@start_timestamp) AND `timestamp` < TIMESTAMP_SECONDS(@end_timestamp)
    """


def _create_query_parameters_for_interval(start_timestamp: int, end_timestamp: int):
    return [
        bigquery.ScalarQueryParameter("start_timestamp", "INT64", start_timestamp),
        bigquery.ScalarQueryParameter("end_timestamp", "INT64", end_timestamp),
    ]


def _deduplicate_table(bq_client: IBqClient, bq_dataset: str, table: str):
    logging.info(f"Deduplicating table: {table}")

    query = _create_query_for_deduplicate_tabel(bq_dataset, table)
    bq_client.run_query([], query, into_table=f"{bq_dataset}.{table}")


def _create_query_for_deduplicate_tabel(dataset: str, table: str):
    return f"""
    SELECT * EXCEPT(`row_number`)
    FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY `_id`) `row_number` FROM `{dataset}.{table}`)
    WHERE `row_number` = 1
    """
