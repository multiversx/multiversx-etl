import datetime
from typing import Any, List, Tuple

import click
from google.cloud import bigquery

from multiversxetl.constants import INDICES_WITH_INTERVALS, SECONDS_IN_ONE_YEAR
from multiversxetl.indexer import Indexer


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--bq-dataset", type=str, help="The BigQuery dataset (destination).")
@click.option("--indexer-url", type=str, help="The indexer URL (Elasticsearch instance).")
@click.option("--tables", multiple=True, default=INDICES_WITH_INTERVALS, help="The tables to run the checks on.")
@click.option("--start-timestamp", type=int, help="The start timestamp (e.g. genesis time).")
@click.option("--end-timestamp", type=int, help="The end timestamp (e.g. a recent one).")
@click.option("--granularity", type=int, default=SECONDS_IN_ONE_YEAR, help="Task granularity, in seconds.")
@click.option("--stop-on-error", is_flag=True, help="Stop on error.")
def check_loaded_data(gcp_project_id: str, bq_dataset: str, indexer_url: str, tables: Tuple[str, ...], start_timestamp: int, end_timestamp: int, granularity: int, stop_on_error: bool):
    bq_client = bigquery.Client(project=gcp_project_id)
    indexer = Indexer(indexer_url)

    for table in list(tables):
        for start in range(start_timestamp, end_timestamp, granularity):
            end = min(start + granularity, end_timestamp)

            start_datetime = datetime.datetime.utcfromtimestamp(start)
            end_datetime = datetime.datetime.utcfromtimestamp(end)
            print(f"Checking table = {table}, start = {start} {(start_datetime)}, end = {(end_datetime)}")

            counts_match = _check_counts_indexer_vs_bq_in_interval(indexer, bq_client, bq_dataset, table, start, end)
            any_duplicates = _check_any_duplicates_in_bq(bq_client, bq_dataset, table, start, end)

            if not counts_match:
                print(f"🗙 Counts do not match.")
            else:
                print(f"✓ Counts match.")

            if any_duplicates:
                print(f"🗙 Duplicates found.")
            else:
                print(f"✓ No duplicates.")

            if not counts_match or any_duplicates:
                if stop_on_error:
                    print("Stopping on error.")
                    return


def _check_counts_indexer_vs_bq_in_interval(indexer: Indexer, bq_client: bigquery.Client, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> bool:
    count_in_indexer = indexer.count_records_with_interval(table, start_timestamp, end_timestamp)
    count_in_bq = _get_num_records_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

    print(f"Count in indexer: {count_in_indexer}")
    print(f"Count in BigQuery: {count_in_bq}")

    return count_in_indexer == count_in_bq


def _check_any_duplicates_in_bq(bq_client: bigquery.Client, bq_dataset: str, table: str, start_timestamp: int, end_timestamp: int) -> bool:
    samples = _get_samples_of_duplicates_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)
    num_duplicates = _get_num_duplicates_in_interval(bq_client, bq_dataset, table, start_timestamp, end_timestamp)

    print(f"Number of duplicates in BigQuery: {num_duplicates}")

    for record in samples:
        print(f"[sample] {table}: ID = {record._id}, count = {record.count}")

    return len(samples) > 0


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
    return records[0].num_duplicates


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


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--gcp-project-id", type=str, help="The GCP project ID.")
@click.option("--bq-dataset", type=str, help="The BigQuery dataset (destination).")
@click.option("--tables", multiple=True, default=INDICES_WITH_INTERVALS, help="The tables to run de-duplication on.")
def deduplicate_loaded_data(gcp_project_id: str, bq_dataset: str, tables: Tuple[str, ...]):
    bq_client = bigquery.Client(project=gcp_project_id)

    for table in list(tables):
        print(f"De-duplicating table {table}...")
        _deduplicate_table(bq_client, bq_dataset, table)


def _deduplicate_table(bq_client: bigquery.Client, bq_dataset: str, table: str):
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
