import json
import logging
import os
import zipfile
from pathlib import Path
from typing import Any, Dict

from utils.error_handling import log_and_raise_exception

ENCODING = "utf-8"


def get_root_path() -> Path:
    """Return the absolute path to the project root directory ending with /."""
    return Path(__file__).parent.parent.parent.absolute()


def ensure_path_exists(path: Path) -> None:
    """Create the directory if it doesn't exist."""
    if path.is_file() or path.suffix:
        directory = path.parent
    else:
        directory = path
    directory.mkdir(parents=True, exist_ok=True)


def load_file(path: Path) -> str | None:
    """
    Opens file and returns stripped content.
    Returns None if file doesnt exist.
    """
    if not path.exists():
        return None

    try:
        with open(path, "r+", encoding=ENCODING) as file:
            data = file.read().strip()
        return data
    except Exception as e:
        log_and_raise_exception("ERROR loading file:  ", e)


def load_json_file(path: Path) -> Dict[str, Any] | None:
    """
    Opens json file and returns it.
    Returns None if file doesn't exist.
    """
    if not path.exists():
        return None

    try:
        with open(path, "r", encoding=ENCODING) as file:
            data = json.load(file)
        return data
    except Exception as e:
        log_and_raise_exception("ERROR loading json:  ", e)


def write_file(path: Path, content: str) -> None:
    """Writes content to file."""
    try:
        with open(path, "w+", encoding=ENCODING) as file:
            file.write(content)
    except Exception as e:
        log_and_raise_exception("ERROR writing file:  ", e)


def unpack_and_remove_zip(zip_path: Path) -> None:
    """
    Unpacks a zip file given a path and places all contents in the same directory.
    Deletes the original zip file.
    """
    try:
        save_directory = os.path.dirname(zip_path)
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(save_directory)
            logging.info(f"Extracted all files to: {save_directory}")

        os.remove(zip_path)
        logging.info(f"Removed the zip file: {zip_path}")
    except Exception as e:
        log_and_raise_exception("ERROR on handling the zip:  ", e)


if __name__ == "__main__":
    print(get_root_path())
