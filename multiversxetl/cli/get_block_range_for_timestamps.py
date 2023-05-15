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
@click.option("-s", "--start-timestamp", required=True, type=int, help="The start UNIX timestamp (in seconds).")
@click.option("-e", "--end-timestamp", required=True, type=int, help="The end UNIX timestamp (in seconds).")
@click.option("-o", "--output", default="-", show_default=True, type=str, help="The output file. If not specified, 'stdout' is used.")
def get_block_range_for_timestamps(provider_uri: str, start_timestamp: int, end_timestamp: int, output: Any):
    """Outputs start and end blocks (hyperblocks) for given timestamps."""
    provider = MultiversXNetworkProvider(provider_uri)
    service = MultiversXBlocksService(provider)
    start_block, end_block = service.get_block_range_for_timestamps(start_timestamp, end_timestamp)

    output_file: Any
    with smart_open(output, "w") as output_file:
        output_file.write("{},{}\n".format(start_block, end_block))
