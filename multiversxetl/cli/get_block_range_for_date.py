import datetime
from typing import Any

import click
from blockchainetl_common.file_utils import smart_open  # type: ignore
from blockchainetl_common.logging_utils import \
    logging_basic_config  # type: ignore

from multiversxetl.providers import MultiversXNetworkProvider
from multiversxetl.service import MultiversXBlocksService

logging_basic_config()


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("-p", "--provider-uri", default="https://gateway.multiversx.com", show_default=True, type=str, help="The URI of the network provider (gateway).")
@click.option("-d", "--date", required=True, type=str, help="The start UNIX timestamp (in seconds).")
@click.option("-o", "--output", default="-", show_default=True, type=str, help="The output file. If not specified, 'stdout' is used.")
def get_block_range_for_date(provider_uri: str, date: str, output: Any):
    """Outputs start and end blocks (hyperblocks) for a date."""
    provider = MultiversXNetworkProvider(provider_uri)
    service = MultiversXBlocksService(provider)
    date_parsed = datetime.datetime.strptime(date, '%Y-%m-%d')
    start_block, end_block = service.get_block_range_for_date(date_parsed)

    output_file: Any
    with smart_open(output, "w") as output_file:
        output_file.write("{},{}\n".format(start_block, end_block))
