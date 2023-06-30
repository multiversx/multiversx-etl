import json
from pathlib import Path
from typing import Any, Dict, Protocol


class IFileStorage(Protocol):
    def get_extracted_path(self, task_pretty_name: str) -> Path: ...
    def get_transformed_path(self, task_pretty_name: str) -> Path: ...


class ITask(Protocol):
    @property
    def index_name(self) -> str: ...
    def get_pretty_name(self) -> str: ...


class TransformJob:
    def __init__(self, file_storage: IFileStorage, task: ITask) -> None:
        self.file_storage = file_storage
        self.task = task
        self.transformers: Dict[str, Transformer] = dict()

        self._register_transformer("blocks", BlocksTransformer())
        self._register_transformer("tokens", TokensTransformer())
        self._register_transformer("logs", LogsTransformer())

    def _register_transformer(self, index_name: str, transformer: 'Transformer') -> None:
        self.transformers[index_name] = transformer

    def run(self) -> None:
        transformer = self.transformers.get(self.task.index_name, Transformer())
        input_filename = self.file_storage.get_extracted_path(self.task.get_pretty_name())
        output_filename = self.file_storage.get_transformed_path(self.task.get_pretty_name())

        with open(input_filename) as file:
            with open(output_filename, "w") as output_file:
                for line in file:
                    transformed_line = transformer.transform(line)
                    output_file.write(transformed_line + "\n")


class Transformer:
    def transform(self, raw_json: str) -> str:
        data = json.loads(raw_json)
        data = self._do_transform(data)
        output = json.dumps(data)
        return output

    def _do_transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        return data


class BlocksTransformer(Transformer):
    def _do_transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data.pop("pubKeyBitmap", None)
        return data


class TokensTransformer(Transformer):
    def _do_transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        data.pop("nft_traitValues", None)
        data.pop("nft_scamInfoType", None)
        data.pop("nft_scamInfoDescription", None)
        return data


class LogsTransformer(Transformer):
    def _do_transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        events = data.get("events", []) or []

        for event in events:
            topics = event.get("topics", []) or []
            # Replace NULL values with empty strings, since BigQuery does not support NULL values in arrays (mode = REPEATED).
            event["topics"] = [topic if topic is not None else "" for topic in topics]

        # We've altered the data in-place.
        return data
