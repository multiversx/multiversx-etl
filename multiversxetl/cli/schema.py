import json
from pathlib import Path

import click

from multiversxetl.schema import map_elastic_search_schema_to_bigquery_schema


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--input-folder", type=str, help="The path to 'input' schema files. E.g. 'elasticreindexer/cmd/indices-creator/config/noKibana'.")
@click.option("--output-folder", type=str, help="The path to 'output' schema files (generated).")
def generate_schema(input_folder: str, output_folder: str):
    input_folder_path = Path(input_folder)
    input_files = list(input_folder_path.glob("*.json"))

    print(f"Found {len(input_files)} input files in {input_folder_path}")

    output_folder_path = Path(output_folder)
    output_folder_path.mkdir(parents=True, exist_ok=True)

    for input_file in input_files:
        input_schema = json.loads(input_file.read_text())
        output_schema = map_elastic_search_schema_to_bigquery_schema(input_schema)

        if not output_schema:
            print("No schema, skipping file:", input_file)
            continue

        output_file_path = output_folder_path / input_file.name
        output_file_path.write_text(json.dumps(output_schema, indent=4) + "\n")
