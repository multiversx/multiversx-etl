
from typing import Optional

import click
from blockchainetl_common.logging_utils import \
    logging_basic_config  # type: ignore

from multiversxetl.jobs.export_blocks_job import ExportBlocksJob
from multiversxetl.jobs.exporters import blocks_and_transactions_item_exporter
from multiversxetl.providers.proxy import MultiversXNetworkProvider

logging_basic_config()


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("-p", "--provider-uri", default="https://gateway.multiversx.com", show_default=True, type=str, help="The URI of the network provider (gateway).")
@click.option("-s", "--start-block", default=0, show_default=True, type=int, help="Start block")
@click.option("-e", "--end-block", required=True, type=int, help="End block")
@click.option("-b", "--batch-size", default=1, show_default=True, type=int, help="The number of blocks to export at a time.")
@click.option("-w", "--max-workers", default=5, show_default=True, type=int, help="The maximum number of workers.")
@click.option("--blocks-output", default=None, show_default=True, type=str, help="The output file for blocks. If not provided, blocks will not be exported.")
@click.option("--transactions-output", default=None, show_default=True, type=str, help="The output file for transactions. If not provided, transactions will not be exported.")
def export_blocks_and_transactions(provider_uri: str, start_block: int, end_block: int,
                                   batch_size: int, max_workers: int,
                                   blocks_output: Optional[str], transactions_output: Optional[str]):
    """Exports blocks (hyperblocks) and transactions."""
    provider = MultiversXNetworkProvider(provider_uri)

    if blocks_output is None and transactions_output is None:
        raise ValueError("At least one of '--blocks-output' and '--transactions-output' options must be provided")

    job = ExportBlocksJob(
        start_block=start_block,
        end_block=end_block,
        batch_size=batch_size,
        provider=provider,
        max_workers=max_workers,
        item_exporter=blocks_and_transactions_item_exporter(blocks_output, transactions_output),
        export_blocks=blocks_output is not None,
        export_transactions=transactions_output is not None)

    job.run()
