from typing import Any

import click

from multiversxetl.cli.checks import check_loaded_data, deduplicate_loaded_data
from multiversxetl.cli.plan_tasks import inspect_tasks, plan_tasks
from multiversxetl.cli.schema import generate_schema
from multiversxetl.cli.work import (extract_with_intervals,
                                    extract_without_intervals,
                                    load_with_intervals,
                                    load_without_intervals)


@click.group()
@click.version_option(version="0.1.0")
@click.pass_context
def cli(ctx: Any):
    pass


cli.add_command(inspect_tasks, "inspect-tasks")
cli.add_command(plan_tasks, "plan-tasks")
cli.add_command(extract_with_intervals, "extract-with-intervals")
cli.add_command(extract_without_intervals, "extract-without-intervals")
cli.add_command(load_with_intervals, "load-with-intervals")
cli.add_command(load_without_intervals, "load-without-intervals")
cli.add_command(generate_schema, "generate-schema")
cli.add_command(check_loaded_data, "check-loaded-data")
cli.add_command(deduplicate_loaded_data, "deduplicate-loaded-data")
