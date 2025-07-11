import datetime
import logging
import os
import sys
import time
from argparse import ArgumentParser
from pathlib import Path
from typing import Any, List

from multiversxetl.app_controller import AppController
from multiversxetl.checks import check_loaded_data
from multiversxetl.constants import SECONDS_IN_DAY, SECONDS_IN_ONE_HOUR
from multiversxetl.errors import CountsMismatchError, KnownError


def main(args: List[str]) -> int:
    # See: https://github.com/grpc/grpc/issues/28557
    os.environ["GRPC_POLL_STRATEGY"] = "poll"

    try:
        _do_main(args)
        return 0
    except KnownError as error:
        logging.error(error)
        return 1
    except KeyboardInterrupt:
        logging.info("Interrupted by user.")
        return 1


def _do_main(args: List[str]):
    parser = ArgumentParser()

    subparsers = parser.add_subparsers()

    parser.add_argument("--verbose", action="store_true", default=False)

    subparser = subparsers.add_parser("process-append-only-indices", help="Do ETL for append-only indices (continuously).")
    subparser.add_argument("--workspace", required=True, help="Workspace path.")
    subparser.add_argument("--sleep-between-iterations", type=int, default=SECONDS_IN_ONE_HOUR)
    subparser.set_defaults(func=_process_append_only_indices)

    subparser = subparsers.add_parser("process-mutable-indices", help="Do ETL for mutable indices (continuously).")
    subparser.add_argument("--workspace", required=True, help="Workspace path.")
    subparser.add_argument("--sleep-between-iterations", type=int, default=SECONDS_IN_DAY)
    subparser.set_defaults(func=_process_mutable_indices)

    subparser = subparsers.add_parser("rewind", help="Rewind to the latest checkpoint.")
    subparser.add_argument("--workspace", required=True, help="Workspace path.")
    subparser.set_defaults(func=_do_rewind_to_checkpoint)

    subparser = subparsers.add_parser("find-latest-good-checkpoint", help="Finds the latest good checkpoint (when BQ and Elasticsearch data counts match).")
    subparser.add_argument("--workspace", required=True, help="Workspace path.")
    subparser.add_argument("--search-step", type=int, default=SECONDS_IN_DAY, help="Search step (search precision).")
    subparser.set_defaults(func=_do_find_latest_good_checkpoint)

    parsed_args = parser.parse_args(args)

    _setup_logging(parsed_args.verbose)

    parsed_args.func(parsed_args)


def _setup_logging(verbose: bool):
    log_level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(level=log_level, format="[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s]: %(message)s")

    # Suppress some logging from Elasticsearch.
    logging.getLogger("elasticsearch.helpers").setLevel(logging.WARNING)
    logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)


def _process_append_only_indices(args: Any):
    workspace = Path(args.workspace).expanduser().resolve()
    sleep_between_iterations = args.sleep_between_iterations

    # Before starting the ETL process, we rewind to the latest checkpoint,
    # to clean up any eventual partial loads from a previous (interrupted) run.
    AppController(workspace).rewind_to_checkpoint()

    for iteration_index in range(0, sys.maxsize):
        logging.info(f"Starting iteration {iteration_index} (_process_append_only_indices)...")

        # We create a new controller on each iteration, so that workspace configuration and state is reloaded.
        controller = AppController(workspace)
        controller.process_append_only_indices()
        controller.bq_client.trigger_data_transfer(controller.worker_config.append_only_indices.bq_data_transfer_name)

        logging.info(f"Iteration {iteration_index} done (_process_append_only_indices). Will sleep {sleep_between_iterations} seconds...")
        time.sleep(sleep_between_iterations)


def _process_mutable_indices(args: Any):
    workspace = Path(args.workspace).expanduser().resolve()
    sleep_between_iterations = args.sleep_between_iterations

    for iteration_index in range(0, sys.maxsize):
        logging.info(f"Starting iteration {iteration_index} (_do_main_mutable_indices)...")

        controller = AppController(workspace)
        controller.process_mutable_indices()
        controller.bq_client.trigger_data_transfer(controller.worker_config.mutable_indices.bq_data_transfer_name)

        logging.info(f"Iteration {iteration_index} done (_do_main_mutable_indices). Will sleep {sleep_between_iterations} seconds...")
        time.sleep(sleep_between_iterations)


def _do_rewind_to_checkpoint(args: Any):
    workspace = Path(args.workspace).expanduser().resolve()
    controller = AppController(workspace)
    controller.rewind_to_checkpoint()


def _do_find_latest_good_checkpoint(args: Any):
    workspace = Path(args.workspace).expanduser().resolve()
    controller = AppController(workspace)
    search_step = args.search_step

    indices_config = controller.worker_config.append_only_indices
    start_timestamp = indices_config.time_partition_start
    now = int(_get_now().timestamp())

    for end_timestamp in range(now, start_timestamp, -search_step):
        try:
            check_loaded_data(
                bq_client=controller.bq_client,
                bq_dataset=indices_config.bq_dataset,
                indexer=controller.indexer,
                tables=indices_config.indices,
                start_timestamp=start_timestamp,
                end_timestamp=end_timestamp,
                use_global_counts_for_bq=False,
                should_fail_on_counts_mismatch=True,
                skip_counts_check_for_indices=[],
                counts_checks_errata=indices_config.counts_checks_errata
            )

            logging.info(f"Latest good checkpoint: {end_timestamp}")
            break
        except CountsMismatchError:
            logging.info("Will try again with an earlier checkpoint...")
            continue


def _get_now() -> datetime.datetime:
    return datetime.datetime.now(tz=datetime.timezone.utc)


if __name__ == "__main__":
    return_code = main(sys.argv[1:])
    sys.exit(return_code)
