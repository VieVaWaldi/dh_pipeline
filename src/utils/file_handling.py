import json
import logging
import os
import zipfile
from typing import Any, Dict

ENCODING = "utf-8"


def ensure_path(path: str) -> None:
    """Ensures the path exists and creates it if it doesn't exist."""
    if not os.path.exists(path):
        os.makedirs(path)


def load_file(path: str) -> str | None:
    """
    Opens file and returns stripped content.
    Returns None if file doesnt exist.
    """
    if not os.path.exists(path):
        return None

    with open(path, "r+", encoding=ENCODING) as file:
        data = file.read().strip()
    return data


def load_json_file(path: str) -> Dict[str, Any] | None:
    """
    Opens json file and returns it.
    Returns None if file doesn't exist.
    """
    if not os.path.exists(path):
        return None

    with open(path, "r", encoding=ENCODING) as file:
        data = json.load(file)
    return data


def write_file(path: str, content: str) -> None:
    """Writes content to file."""
    with open(path, "w+", encoding=ENCODING) as file:
        file.write(content)


def unpack_and_remove_zip(zip_path: str) -> None:
    """
    Unpacks a zip file given a path and places all contents in the same directory.
    Deletes the original zip file.
    """
    save_directory = os.path.dirname(zip_path)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(save_directory)
        logging.info("Extracted all files to: %s", save_directory)

    os.remove(zip_path)
    logging.info("Removed the zip file: %s", zip_path)
