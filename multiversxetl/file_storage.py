from pathlib import Path


class FileStorage:
    def __init__(self, base_folder: Path) -> None:
        self.extracted_folder = base_folder / "extracted"
        self.transformed_folder = base_folder / "transformed"

        self.extracted_folder.mkdir(parents=True, exist_ok=True)
        self.transformed_folder.mkdir(parents=True, exist_ok=True)

    def get_extracted_path(self, task_pretty_name: str) -> Path:
        return self.extracted_folder / f"{task_pretty_name}_extracted.json"

    def get_transformed_path(self, task_pretty_name: str) -> Path:
        return self.transformed_folder / f"{task_pretty_name}_transformed.json"

    def get_load_path(self, task_pretty_name: str):
        transformed_path = self.get_transformed_path(task_pretty_name)
        extracted_path = self.get_extracted_path(task_pretty_name)

        if transformed_path.exists():
            return transformed_path
        return extracted_path

    def remove_extracted_file(self, task_pretty_name: str):
        extracted_path = self.get_extracted_path(task_pretty_name)
        extracted_path.unlink(missing_ok=True)

    def remove_transformed_file(self, task_pretty_name: str):
        transformed_path = self.get_transformed_path(task_pretty_name)
        transformed_path.unlink(missing_ok=True)
