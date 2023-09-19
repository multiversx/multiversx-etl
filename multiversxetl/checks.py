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
    should_fail_on_counts_mismatch: bool
):

    for table in tables:
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

    any_duplicates: bool = _check_any_duplicates_in_bq(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

    if any_duplicates:
        logging.warning(f"Duplicates found (will be corrected).")
        _deduplicate_table(bq_client, bq_dataset, table)

    any_duplicates: bool = _check_any_duplicates_in_bq(bq_client, bq_dataset, table, start_timestamp, end_timestamp)
    assert not any_duplicates

    counts_match: bool = _check_counts_indexer_vs_bq_in_interval(indexer, bq_client, bq_dataset, table, start_timestamp, end_timestamp)
    if not counts_match and should_fail_on_counts_mismatch:
        raise Exception(f"Counts do not match for '{table}'.")


def _check_counts_indexer_vs_bq_in_interval(indexer: IIndexer, bq_client: IBqClient, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> bool:
    count_in_indexer = indexer.count_records(table, start_timestamp, end_timestamp)
    count_in_bq = _get_num_records_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

    if count_in_indexer != count_in_bq:
        logging.warning(f"Counts do not match for '{table}': indexer = {count_in_indexer}, bq = {count_in_bq}.")
    else:
        logging.info(f"Counts match for '{table}': {count_in_indexer}")

    return count_in_indexer == count_in_bq


def _check_any_duplicates_in_bq(bq_client: IBqClient, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> bool:
    num_duplicates = _get_num_duplicates_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

    if num_duplicates:
        logging.warning(f"Number of duplicates in BigQuery: {num_duplicates}")

        samples = _get_samples_of_duplicates_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

        for record in samples:
            logging.debug(f"Duplicate sample for {table}: ID = {record._id}, count = {record.count}")

    return num_duplicates > 0


def _get_samples_of_duplicates_in_interval(bq_client: IBqClient, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> List[Any]:
    query = _create_query_for_get_samples_of_duplicates_in_interval(bq_dataset, table)
    query_parameters = _create_query_parameters_for_interval(start_timestamp, end_timestamp)
    records = bq_client.run_query(query_parameters, query)
    return records


def _create_query_for_get_samples_of_duplicates_in_interval(dataset: str, table: str):
    return f"""
    SELECT `_id`, COUNT(*) AS `count`
    FROM `{dataset}.{table}`
    WHERE `timestamp` >= TIMESTAMP_SECONDS(@start_timestamp) AND `timestamp` < TIMESTAMP_SECONDS(@end_timestamp)
    GROUP BY _id
    HAVING `count` > 1
    LIMIT 10
    """


def _get_num_duplicates_in_interval(bq_client: IBqClient, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> int:
    query = _create_query_for_get_num_duplicates_in_interval(bq_dataset, table)
    query_parameters = _create_query_parameters_for_interval(start_timestamp, end_timestamp)
    records = bq_client.run_query(query_parameters, query)
    return records[0].num_duplicates or 0


def _create_query_for_get_num_duplicates_in_interval(dataset: str, table: str):
    return f"""
    WITH `counts` AS (
        SELECT COUNT(*) AS `count`
        FROM `{dataset}.{table}`
        WHERE `timestamp` >= TIMESTAMP_SECONDS(@start_timestamp) AND `timestamp` < TIMESTAMP_SECONDS(@end_timestamp)
        GROUP BY _id
        HAVING `count` > 1
    )
    SELECT (SUM(`count`) - COUNT(*)) AS `num_duplicates` FROM `counts`
    """


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
