import os
from pathlib import Path
from typing import Any, Dict

from dotenv import load_dotenv

from utils.file_handling import load_json_file

load_dotenv()


def get_config() -> Dict[str, Any]:
    """Returns default config as json"""
    return _get_config("config")


def get_query_config() -> Dict[str, Any]:
    """Returns query config as json"""
    return _get_config("config_queries")


def _get_config(config_name: str) -> Dict[str, Any]:
    """Prepares and returns the given config as json"""
    config_file_path = (
        Path(__file__).resolve().parent.parent.parent
        / "configs"
        / f"{config_name}.json"
    )

    config = load_json_file(str(config_file_path))
    if not config:
        raise Exception("Error reading the logging file.")

    env = os.getenv("ENV")
    if env not in config:
        return config
    return config[env]
