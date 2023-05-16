from typing import Any

import click

from multiversxetl.cli.get_block_range_for_date import \
    get_hyperblock_range_for_date
from multiversxetl.cli.get_block_range_for_timestamps import \
    get_hyperblock_range_for_timestamps


@click.group()
@click.version_option(version="0.1.0")
@click.pass_context
def cli(ctx: Any):
    pass


cli.add_command(get_hyperblock_range_for_date, "get_block_range_for_date")
cli.add_command(get_hyperblock_range_for_timestamps, "get_block_range_for_timestamps")
