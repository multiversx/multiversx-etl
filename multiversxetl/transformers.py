
import json
from typing import Any, Dict


class TransformersRegistry:
    def __init__(self):
        self.trivial_transformer = Transformer()
        self.transformers: Dict[str, Transformer] = {
            "accounts": AccountsTransformer(),
            "blocks": BlocksTransformer(),
            "tokens": TokensTransformer(),
            "logs": LogsTransformer()
        }

    def get_transformer(self, index_name: str) -> 'Transformer':
        return self.transformers.get(index_name, self.trivial_transformer)


class Transformer:
    def transform_json(self, raw_json: str) -> str:
        data = json.loads(raw_json)
        data = self.transform(data)
        output = json.dumps(data)
        return output

    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return data


class AccountsTransformer(Transformer):
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        for key in list(data.keys()):
            is_volatile_field_api = key.startswith("api_")

            if is_volatile_field_api:
                data.pop(key)

        return data


class BlocksTransformer(Transformer):
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data.pop("pubKeyBitmap", None)
        data.pop("reserved", None)

        # Remove "epochStartShardsData.pendingMiniBlockHeaders.reserved".
        for shard_data in data.get("epochStartShardsData", []):
            for miniblock_header in shard_data.get("pendingMiniBlockHeaders", []):
                miniblock_header.pop("reserved", None)

        return data


class TokensTransformer(Transformer):
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        for key in list(data.keys()):
            is_volatile_field_nft = key.startswith("nft_")
            is_volatile_field_api = key.startswith("api_")

            if is_volatile_field_nft or is_volatile_field_api:
                data.pop(key)

        return data


class LogsTransformer(Transformer):
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        events = data.get("events", []) or []

        for event in events:
            topics = event.get("topics", []) or []
            additional_data = event.get("additionalData", []) or []

            # Replace NULL values with empty strings, since BigQuery does not support NULL values in arrays (mode = REPEATED).
            event["topics"] = [topic if topic is not None else "" for topic in topics]
            event["additionalData"] = [data_item if data_item is not None else "" for data_item in additional_data]

        # We've altered the data in-place.
        return data
