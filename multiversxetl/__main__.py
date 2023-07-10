import logging
import os
import sys

from multiversxetl.cli import cli
from multiversxetl.errors import UsageError

logging.basicConfig(level=logging.INFO, format="[%(threadName)s] [%(levelname)s] [%(module)s]: %(message)s")

if __name__ == "__main__":
    # See: https://github.com/grpc/grpc/issues/28557
    os.environ["GRPC_POLL_STRATEGY"] = "poll"

    try:
        cli()
    except UsageError as error:
        logging.error(error)
        sys.exit(1)
