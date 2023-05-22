import json
from typing import Any, Dict, Protocol


class ITask(Protocol):
    @property
    def index_name(self) -> str: ...
    def get_extraction_filename(self) -> str: ...
    def get_transformed_filename(self) -> str: ...


class TransformJob:
    def __init__(self, task: ITask) -> None:
        self.task = task
        self.transformers: Dict[str, BlocksTransformer] = {
            "blocks": BlocksTransformer()
        }

    def run(self) -> None:
        transformer = self.transformers.get(self.task.index_name)
        if not transformer:
            return

        input_filename = self.task.get_extraction_filename()
        output_filename = self.task.get_transformed_filename()

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
        del data["validators"]
        del data["pubKeyBitmap"]
        return data
