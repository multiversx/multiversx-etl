import json
import logging
import os
import sys
import time
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, List

from multiversxetl.app_controller import AppController
from multiversxetl.constants import SECONDS_IN_DAY, SECONDS_IN_ONE_HOUR
from multiversxetl.errors import UsageError
from multiversxetl.schema import map_elastic_search_schema_to_bigquery_schema

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s]: %(message)s")
# Suppress some logging from Elasticsearch.
logging.getLogger("elasticsearch.helpers").setLevel(logging.WARNING)
logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)


def main(args: List[str]) -> int:
    # See: https://github.com/grpc/grpc/issues/28557
    os.environ["GRPC_POLL_STRATEGY"] = "poll"

    try:
        _do_main(args)
        return 0
    except UsageError as error:
        logging.error(error)
        return 1
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
        return 1


def _do_main(args: List[str]):
    parser = ArgumentParser()

    subparsers = parser.add_subparsers()

    subparser = subparsers.add_parser("etl-append-only-indices", help="Do ETL for append-only indices (continuously).")
    subparser.add_argument("--workspace", required=True, help="Workspace path.")
    subparser.add_argument("--sleep-between-iterations", type=int, default=SECONDS_IN_ONE_HOUR)
    subparser.set_defaults(func=_do_etl_append_only_indices)

    subparser = subparsers.add_parser("etl-mutable-indices", help="Do ETL for mutable indices (continuously).")
    subparser.add_argument("--workspace", required=True, help="Workspace path.")
    subparser.add_argument("--sleep-between-iterations", type=int, default=SECONDS_IN_DAY)
    subparser.set_defaults(func=_do_etl_mutable_indices)

    subparser = subparsers.add_parser("rewind", help="Rewind to the latest checkpoint.")
    subparser.add_argument("--workspace", required=True, help="Workspace path.")
    subparser.set_defaults(func=_do_rewind_to_checkpoint)

    subparser = subparsers.add_parser("check-append-only-indices", help="..")
    subparser.add_argument("--workspace", required=True, help="Workspace path.")
    subparser.set_defaults(func=_do_check_append_only_indices)

    subparser = subparsers.add_parser("regenerate-schema", help="Re-generates BQ schema files from ES schema files.")
    subparser.add_argument("--input-folder", type=str, help="The path to 'input' schema files. E.g. 'elasticreindexer/cmd/indices-creator/config/noKibana'.")
    subparser.add_argument("--output-folder", type=str, help="The path to 'output' schema files (generated).")
    subparser.set_defaults(func=_do_regenerate_schema)

    parsed_args = parser.parse_args(args)
    parsed_args.func(parsed_args)


def _do_etl_append_only_indices(args: Any):
    workspace = Path(args.workspace).expanduser().resolve()
    sleep_between_iterations = args.sleep_between_iterations

    for iteration_index in range(0, sys.maxsize):
        logging.info(f"Starting iteration {iteration_index} (_do_main_etl_append_only_indices)...")

        controller = AppController(workspace)
        controller.etl_append_only_indices()
        controller.bq_client.trigger_data_transfer(controller.worker_config.append_only_indices.bq_data_transfer_name)

        logging.info(f"Iteration {iteration_index} done (_do_main_etl_append_only_indices). Will sleep {sleep_between_iterations} seconds...")
        time.sleep(sleep_between_iterations)


def _do_etl_mutable_indices(args: Any):
    workspace = Path(args.workspace).expanduser().resolve()
    sleep_between_iterations = args.sleep_between_iterations

    for iteration_index in range(0, sys.maxsize):
        logging.info(f"Starting iteration {iteration_index} (_do_main_mutable_indices)...")

        controller = AppController(workspace)
        controller.etl_mutable_indices()
        controller.bq_client.trigger_data_transfer(controller.worker_config.mutable_indices.bq_data_transfer_name)

        logging.info(f"Iteration {iteration_index} done (_do_main_mutable_indices). Will sleep {sleep_between_iterations} seconds...")
        time.sleep(sleep_between_iterations)


def _do_rewind_to_checkpoint(args: Any):
    workspace = Path(args.workspace).expanduser().resolve()
    controller = AppController(workspace)
    controller.rewind_to_checkpoint()


def _do_regenerate_schema(args: Any):
    input_folder = Path(args.input_folder).expanduser().resolve()
    output_folder = Path(args.output_folder).expanduser().resolve()

    input_folder_path = Path(input_folder)
    input_files = list(input_folder_path.glob("*.json"))

    print(f"Found {len(input_files)} input files in {input_folder_path}")

    output_folder_path = Path(output_folder)
    output_folder_path.mkdir(parents=True, exist_ok=True)

    for input_file in input_files:
        input_schema = json.loads(input_file.read_text())
        output_schema = map_elastic_search_schema_to_bigquery_schema(input_schema)

        if not output_schema:
            print("No schema, skipping file:", input_file)
            continue

        output_file_path = output_folder_path / input_file.name
        output_file_path.write_text(json.dumps(output_schema, indent=4) + "\n")


if __name__ == "__main__":
    return_code = main(sys.argv[1:])
    sys.exit(return_code)
