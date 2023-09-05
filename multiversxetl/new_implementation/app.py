import logging
import os
import sys
from argparse import ArgumentParser
from pathlib import Path
from typing import List

from multiversxetl.errors import UsageError
from multiversxetl.new_implementation.worker_config import WorkerConfig
from multiversxetl.new_implementation.worker_state import WorkerState

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

    parsed_args = parser.parse_args(args)

    workspace = Path(parsed_args.workspace).expanduser().resolve()
    worker_config_path = workspace / "worker_config.json"
    worker_state_path = workspace / "worker_state.json"

    if not worker_config_path.exists():
        raise UsageError(f"Worker config file not found: {worker_config_path}")
    if not worker_state_path.exists():
        raise UsageError(f"Worker state file not found: {worker_state_path}")

    worker_config = WorkerConfig.load_from_file(worker_config_path)
    worker_state = WorkerState.load_from_file(worker_state_path)


if __name__ == "__main__":
    return_code = main(sys.argv[1:])
    sys.exit(return_code)
