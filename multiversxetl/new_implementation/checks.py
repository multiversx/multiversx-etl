import datetime
import logging
from typing import Any, List, Protocol

from google.cloud import bigquery


class IIndexer(Protocol):
    def count_records_with_interval(self, index_name: str, start_timestamp: int, end_timestamp: int) -> int: ...


def check_loaded_data(
        bq_client: bigquery.Client,
        bq_dataset: str,
        indexer: IIndexer,
        tables: List[str],
        start_timestamp: int,
        end_timestamp: int):

    for table in tables:
        start_datetime = datetime.datetime.utcfromtimestamp(start_timestamp)
        end_datetime = datetime.datetime.utcfromtimestamp(end_timestamp)
        print(f"Checking table = {table}, start = {start_timestamp} {(start_datetime)}, end = {(end_datetime)}")

        any_duplicates: bool = _check_any_duplicates_in_bq(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

        if any_duplicates:
            print(f"🗙 Duplicates found (will be corrected).")
            _deduplicate_table(bq_client, bq_dataset, table)

        any_duplicates: bool = _check_any_duplicates_in_bq(bq_client, bq_dataset, table, start_timestamp, end_timestamp)
        assert not any_duplicates

    for table in tables:
        start_datetime = datetime.datetime.utcfromtimestamp(start_timestamp)
        end_datetime = datetime.datetime.utcfromtimestamp(end_timestamp)
        print(f"Checking table = {table}, start = {start_timestamp} {(start_datetime)}, end = {(end_datetime)}")

        counts_match: bool = _check_counts_indexer_vs_bq_in_interval(indexer, bq_client, bq_dataset, table, start_timestamp, end_timestamp)
        if not counts_match:
            raise Exception(f"Counts do not match for '{table}'.")


def _check_counts_indexer_vs_bq_in_interval(indexer: IIndexer, bq_client: bigquery.Client, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> bool:
    count_in_indexer = indexer.count_records_with_interval(table, start_timestamp, end_timestamp)
    count_in_bq = _get_num_records_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

    if count_in_indexer != count_in_bq:
        logging.warning(f"Counts do not match for '{table}': indexer = {count_in_indexer}, bq = {count_in_bq}.")

    return count_in_indexer == count_in_bq


def _check_any_duplicates_in_bq(bq_client: bigquery.Client, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> bool:
    num_duplicates = _get_num_duplicates_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

    if num_duplicates:
        print(f"Number of duplicates in BigQuery: {num_duplicates}")

        samples = _get_samples_of_duplicates_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

        for record in samples:
            print(f"[sample] {table}: ID = {record._id}, count = {record.count}")

    return num_duplicates > 0


def _get_samples_of_duplicates_in_interval(bq_client: bigquery.Client, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> List[Any]:
    query = _create_query_for_get_samples_of_duplicates_in_interval(bq_dataset, table)
    query_parameters = _create_query_parameters_for_interval(start_timestamp, end_timestamp)
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    job = bq_client.query(query, job_config=job_config)
    records = list(job.result())
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


def _get_num_duplicates_in_interval(bq_client: bigquery.Client, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> int:
    query = _create_query_for_get_num_duplicates_in_interval(bq_dataset, table)
    query_parameters = _create_query_parameters_for_interval(start_timestamp, end_timestamp)
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    job = bq_client.query(query, job_config=job_config)
    records = list(job.result())
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


def _get_num_records_in_interval(bq_client: bigquery.Client, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> int:
    query = _create_query_for_get_num_records_in_interval(bq_dataset, table)
    query_parameters = _create_query_parameters_for_interval(start_timestamp, end_timestamp)
    job_config = bigquery.QueryJobConfig(query_parameters=query_parameters)
    job = bq_client.query(query, job_config=job_config)
    records = list(job.result())
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


def _deduplicate_table(bq_client: bigquery.Client, bq_dataset: str, table: str):
    logging.info(f"Deduplicating table: {table}")

    query = _create_query_for_deduplicate_tabel(bq_dataset, table)
    job_config = bigquery.QueryJobConfig(
        destination=f"{bq_client.project}.{bq_dataset}.{table}",
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    job = bq_client.query(query, job_config=job_config)
    job.result()


def _create_query_for_deduplicate_tabel(dataset: str, table: str):
    return f"""
    SELECT * EXCEPT(`row_number`)
    FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY `_id`) `row_number` FROM `{dataset}.{table}`)
    WHERE `row_number` = 1
    """
