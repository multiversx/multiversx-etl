from typing import Any, Dict, List

BQ_TYPE_BY_ES_TYPE: Dict[str, str] = {
    "boolean": "BOOLEAN",
    "long": "NUMERIC",
    "double": "FLOAT64",
    "integer": "NUMERIC",
    "float": "FLOAT64",
    "keyword": "STRING",
    "text": "STRING",
    "date": "TIMESTAMP"
}

BQ_PRIMARY_KEY = {
    "name": "_id",
    "type": "STRING",
}


def map_elastic_search_schema_to_bigquery_schema(input_schema: Dict[str, Any]) -> List[Dict[str, Any]]:
    input_fields: Dict[str, Any] = input_schema.get("mappings", {}).get("properties", {}) or {}
    if not input_fields:
        return []

    output_fields = _map_fields(input_fields)

    return [BQ_PRIMARY_KEY] + output_fields


def _map_fields(input_fields: Dict[str, Any]) -> List[Dict[str, Any]]:
    output_fields = [_map_field(name, input_field) for name, input_field in input_fields.items()]
    return output_fields


def _map_field(name: str, input_metadata: Dict[str, Any]) -> Dict[str, Any]:
    is_object = input_metadata.get("isObject", False)
    is_array = input_metadata.get("isArray", False)
    input_type = input_metadata.get("type")

    if is_object:
        nested_input_fields = input_metadata["properties"]
        nested_output_fields = _map_fields(nested_input_fields)

        return {
            "name": name,
            **({"mode": "REPEATED"} if is_array else {}),
            "fields": nested_output_fields,
            "type": "RECORD"
        }

    assert input_type, f"Missing type for property: {name}; {input_metadata}"

    if is_array:
        return {
            "name": name,
            "mode": "REPEATED",
            "type": _map_type(input_type)
        }

    return {
        "name": name,
        "type": _map_type(input_type)
    }


def _map_type(input_type: str) -> str:
    output_type = BQ_TYPE_BY_ES_TYPE.get(input_type)
    if not output_type:
        raise Exception(f"Unknown type: {input_type}")

    return output_type
