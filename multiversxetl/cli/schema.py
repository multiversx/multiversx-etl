import json
from pathlib import Path
from pprint import pprint
from typing import Any, Dict, List

import click


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))
@click.option("--input-folder", type=str, help="The path to 'input' schema files. E.g. 'elasticreindexer/cmd/indices-creator/config/noKibana'.")
@click.option("--output-folder", type=str, help="The path to 'output' schema files (generated).")
def generate_schema(input_folder: str, output_folder: str):
    input_folder_path = Path(input_folder)
    output_folder_path = Path(output_folder)

    output_folder_path.mkdir(parents=True, exist_ok=True)

    input_files = list(input_folder_path.glob("*.json"))

    for input_file in input_files:
        input_schema = json.loads(input_file.read_text())
        output_schema: List[Dict[str, Any]] = []
        print(input_file)
        input_properties = input_schema.get("mappings", {}).get("properties", [])

        if not input_properties:
            print("No properties found, skipping.", input_file)
            continue

        output_properties = _map_input_properties_to_schema_fields(input_properties)
        output_schema = [{
            "name": "_id",
            "type": "STRING",
        }] + output_properties

        output_file_path = output_folder_path / input_file.name
        output_file_path.write_text(json.dumps(output_schema, indent=4))


def _map_input_properties_to_schema_fields(input_properties: Dict[str, Any]) -> List[Dict[str, Any]]:
    type_mapping: Dict[str, str] = {
        "boolean": "BOOLEAN",
        "long": "NUMERIC",
        "double": "NUMERIC",
        "integer": "NUMERIC",
        "float": "NUMERIC",
        "keyword": "STRING",
        "text": "STRING",
        "date": "TIMESTAMP"
    }

    output_properties: List[Dict[str, Any]] = []

    for name, input_property in input_properties.items():
        output_property: Dict[str, Any] = {
            "name": name
        }

        input_type = input_property.get("type")
        is_array = input_property.get("isArray", False)
        is_object = input_property.get("isObject", False)

        if not input_type and not is_object:
            pprint(input_property)
            raise Exception(f"Missing type for property: {name}")

        if is_array:
            output_property["mode"] = "REPEATED"

        if is_object:
            nested_properties = input_property["properties"]
            nested_output_properties = _map_input_properties_to_schema_fields(nested_properties)
            output_property["fields"] = nested_output_properties
            output_property["type"] = "RECORD"

            output_properties.append(output_property)
            continue

        output_type = type_mapping.get(input_type)
        if not output_type:
            raise Exception(f"Unknown type: {input_type}")

        output_property["type"] = output_type

        output_properties.append(output_property)

    return output_properties
