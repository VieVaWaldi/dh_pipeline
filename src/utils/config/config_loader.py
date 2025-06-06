import os
from typing import Any, Dict

from dotenv import load_dotenv

from lib.file_handling.json_utils import load_json_file
from lib.file_handling.path_utils import get_project_root_path


def get_config() -> Dict[str, Any]:
    """Returns default config for connections and dev/prod mode as dict"""
    return _get_config("config")


def get_query_config() -> Dict[str, Any]:
    """Returns query config as dict"""
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
