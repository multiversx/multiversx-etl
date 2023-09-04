import logging
import os
import sys

from multiversxetl.cli import cli
from multiversxetl.errors import UsageError

logging.basicConfig(level=logging.INFO, format="[%(asctime)s] [%(levelname)s] [%(threadName)s] [%(module)s]: %(message)s")
# Suppress some logging from Elasticsearch.
logging.getLogger("elasticsearch.helpers").setLevel(logging.WARNING)
logging.getLogger("elastic_transport.transport").setLevel(logging.WARNING)


if __name__ == "__main__":
    # See: https://github.com/grpc/grpc/issues/28557
    os.environ["GRPC_POLL_STRATEGY"] = "poll"

    try:
        cli()
    except UsageError as error:
        logging.error(error)
        sys.exit(1)
