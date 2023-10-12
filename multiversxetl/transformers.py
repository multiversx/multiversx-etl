
import json
from typing import Any, Dict


class Transformer:
    def transform_json(self, raw_json: str) -> str:
        data = json.loads(raw_json)
        data = self.transform(data)
        output = json.dumps(data)
        return output

    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return data


class BlocksTransformer(Transformer):
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data.pop("pubKeyBitmap", None)

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
            # Replace NULL values with empty strings, since BigQuery does not support NULL values in arrays (mode = REPEATED).
            event["topics"] = [topic if topic is not None else "" for topic in topics]

        # We've altered the data in-place.
        return data
