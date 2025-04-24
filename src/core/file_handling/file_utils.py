import json
import logging
import os
import shutil
import zipfile
from pathlib import Path
from typing import Any, Dict, Optional, List

from common_utils.error_handling.error_handling import log_and_raise_exception

ENCODING = "utf-8"


def get_project_root_path() -> Path:
    """Return the absolute path to the project root directory ending with /."""
    return Path(__file__).parent.parent.parent.parent.absolute()


def ensure_path_exists(path: Path) -> None:
    """Create the directory if it doesn't exist. If given a file picks the parent directory."""
    if path.is_file() or path.suffix:
        directory = path.parent
    else:
        directory = path
    directory.mkdir(parents=True, exist_ok=True)


def delete_if_empty(folder_path: Path) -> None:
    """
    Delete the given folder if it's empty.
    """
    if folder_path.is_dir() and not any(folder_path.iterdir()):
        try:
            shutil.rmtree(folder_path)
        except OSError as e:
            log_and_raise_exception(f"Error deleting folder {folder_path}: {e}")


def raise_error_if_directory_does_not_exists(directory_path: Path) -> None:
    """
    Check if a directory exists and raise an error if it doesn't.
    """
    if not directory_path.is_dir():
        log_and_raise_exception(f"The directory does not exist: {directory_path}")


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


def save_json_dict(dictionary: Dict[str, Any], path: Path):
    ensure_path_exists(path)

    try:
        with open(path, "w", encoding=ENCODING) as f:
            json.dump(dictionary, f, indent=4)
    except Exception as e:
        log_and_raise_exception("ERROR saving json:  ", e)


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


def count_files(path: Path, file_type: str = ".xml") -> int:
    count = 0
    for root, dirs, files in os.walk(path):
        for file in files:
            if file.endswith(file_type):
                count += 1
    return count


def find_pdfs_in_directory(file_path: Path) -> Optional[List[Path]]:
    """
    Given a Path to a file, finds all PDF files in the same directory.

    Args:
        file_path (Path): Path to a file

    Returns:
        Optional[List[Path]]: List of Paths to PDF files found in the directory,
                            or None if no PDFs are found or path is invalid
    """
    try:
        directory = file_path.parent

        if not file_path.is_file():
            return None

        pdf_files = list(directory.glob("*.pdf"))
        return pdf_files if pdf_files else None

    except Exception:
        return None


if __name__ == "__main__":
    print(get_project_root_path())
