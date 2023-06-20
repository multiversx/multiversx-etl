from typing import Any

import click

from multiversxetl.cli.plan_tasks import (inspect_tasks,
                                          plan_tasks_with_intervals,
                                          plan_tasks_without_intervals)
from multiversxetl.cli.schema import generate_schema
from multiversxetl.cli.work import (do_extract_with_intervals,
                                    do_extract_without_intervals,
                                    do_load_with_intervals,
                                    do_load_without_intervals)


@click.group()
@click.version_option(version="0.1.0")
@click.pass_context
def cli(ctx: Any):
    pass


cli.add_command(inspect_tasks, "inspect_tasks")
cli.add_command(plan_tasks_with_intervals, "plan_tasks_with_intervals")
cli.add_command(plan_tasks_without_intervals, "plan_tasks_without_intervals")
cli.add_command(do_extract_with_intervals, "do_extract_with_intervals")
cli.add_command(do_extract_without_intervals, "do_extract_without_intervals")
cli.add_command(do_load_with_intervals, "do_load_with_intervals")
cli.add_command(do_load_without_intervals, "do_load_without_intervals")
cli.add_command(generate_schema, "generate_schema")
