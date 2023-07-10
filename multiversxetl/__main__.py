import logging
import sys

from multiversxetl.cli import cli
from multiversxetl.errors import UsageError

logging.basicConfig(level=logging.INFO, format="[%(threadName)s] [%(levelname)s] [%(module)s]: %(message)s")

if __name__ == "__main__":
    try:
        cli()
    except UsageError as error:
        logging.error(error)
        sys.exit(1)
