from typing import Any

import click

from multiversxetl.cli.export_blocks_and_transactions import \
    export_blocks_and_transactions
from multiversxetl.cli.get_block_range_for_date import \
    get_hyperblock_range_for_date
from multiversxetl.cli.get_block_range_for_timestamps import \
    get_hyperblock_range_for_timestamps
from multiversxetl.cli.plan_tasks import (inspect_tasks,
                                          plan_tasks_with_intervals,
                                          plan_tasks_without_intervals)
from multiversxetl.cli.work import (do_extract_with_intervals,
                                    do_extract_without_intervals,
                                    do_load_with_intervals)


@click.group()
@click.version_option(version="0.1.0")
@click.pass_context
def cli(ctx: Any):
    pass


cli.add_command(get_hyperblock_range_for_date, "get_block_range_for_date")
cli.add_command(get_hyperblock_range_for_timestamps, "get_block_range_for_timestamps")
cli.add_command(export_blocks_and_transactions, "export_blocks_and_transactions")
cli.add_command(inspect_tasks, "inspect_tasks")
cli.add_command(plan_tasks_with_intervals, "plan_tasks_with_intervals")
cli.add_command(plan_tasks_without_intervals, "plan_tasks_without_intervals")
cli.add_command(do_extract_with_intervals, "do_extract_with_intervals")
cli.add_command(do_extract_without_intervals, "do_extract_without_intervals")
cli.add_command(do_load_with_intervals, "do_load_with_intervals")
