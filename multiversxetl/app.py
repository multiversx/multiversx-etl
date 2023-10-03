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
    parser.add_argument("--workspace", required=True, help="Workspace path.")

    subparsers = parser.add_subparsers()

    subparser = subparsers.add_parser("etl-append-only-indices", help="Do ETL for append-only indices (continuously).")
    subparser.add_argument("--sleep-between-iterations", type=int, default=SECONDS_IN_ONE_HOUR)
    subparser.set_defaults(func=_do_etl_append_only_indices)

    subparser = subparsers.add_parser("etl-mutable-indices", help="Do ETL for mutable indices (continuously).")
    subparser.add_argument("--sleep-between-iterations", type=int, default=SECONDS_IN_DAY)
    subparser.set_defaults(func=_do_etl_mutable_indices)

    subparser = subparsers.add_parser("rewind", help="Rewind to the latest checkpoint.")
    subparser.set_defaults(func=_do_rewind_to_checkpoint)


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


if __name__ == "__main__":
    return_code = main(sys.argv[1:])
    sys.exit(return_code)
