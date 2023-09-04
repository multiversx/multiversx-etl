from typing import Any

import click

from multiversxetl.cli.checks import check_loaded_data, deduplicate_loaded_data
from multiversxetl.cli.run import run
from multiversxetl.cli.schema import generate_schema


@click.group()
@click.version_option(version="0.1.0")
@click.pass_context
def cli(ctx: Any):
    pass


cli.add_command(run, "run")
cli.add_command(generate_schema, "generate-schema")
cli.add_command(check_loaded_data, "check-loaded-data")
cli.add_command(deduplicate_loaded_data, "deduplicate-loaded-data")
