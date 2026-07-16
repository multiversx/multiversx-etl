from pathlib import Path

from multiversxetl.worker_config import WorkerConfig


CONFIG_FOLDER = Path(__file__).parent / "config"


def test_mainnet_mutable_indices_ignored_fields():
    config = WorkerConfig.load_from_file(CONFIG_FOLDER / "worker_config_mainnet.json")

    assert config.mutable_indices.ignored_fields == [
        "fang_write_test",
        "test_field",
        "test_len",
        "test_map",
        "test_ts",
        "probe"
    ]


def test_ignored_fields_default_to_empty_list():
    config = WorkerConfig.load_from_file(CONFIG_FOLDER / "worker_config_devnet.json")

    assert config.mutable_indices.ignored_fields == []
