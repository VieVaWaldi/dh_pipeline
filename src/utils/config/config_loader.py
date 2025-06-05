import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

from elt.extraction.extractor_utils import clean_extractor_name
from lib.file_handling.file_utils import get_project_root_path, load_json_file


def get_source_data_path(source_name: str, config: Dict, run: int | None) -> Path:
    """
    This function returns the path to the raw source data.
    Gets path from project root for local development.
    """
    assert config, "config must be a non-empty dict"
    assert get_query_config().get(source_name), f"Faulty source name: {source_name}"

    path = (
        Path(config["data_path"])
        / (
            source_name
            + "_"
            + clean_extractor_name(
                get_query_config()[source_name]["runs"][run]["query"]
            )
        )
        if run
        else Path(config["data_path"]) / source_name  # for meta_heritage ETL not ELT
    )

    if os.getenv("ENV") == "dev":
        return get_project_root_path() / path
    return path


def get_config() -> Dict[str, Any]:
    """Returns default config as json"""
    return _get_config("config")


def get_query_config() -> Dict[str, Any]:
    """Returns query config as json"""
    return _get_config("config_queries")


def _get_config(config_name: str) -> Dict[str, Any]:
    """Prepares and returns the given config as json"""
    config_file_path = get_project_root_path() / "config" / f"{config_name}.json"

    config = load_json_file(config_file_path)
    if not config:
        raise Exception("Error reading the logger file.")

    load_dotenv()
    env = os.getenv("ENV")
    if env not in config:
        return config
    return config[env]
