import os
from pathlib import Path
from typing import Iterator, Any

from utils.file_handling.file_parsing.json_parser import (
    extract_json_as_dict,
)
from utils.file_handling.file_parsing.xml_parser import (
    extract_xml_as_dict,
)


def get_all_documents_with_path(path: Path):
    """
    Returns all documents as dictionaries given a path recursively.
    """
    yield from _process_files(path)


def _process_files(file_path: Path) -> Iterator[Any]:
    """
    Walks through all files in the given file_path and extracts the document as a dictionary.
    """
    for root, _, files in os.walk(file_path):
        for file in files:
            full_file_path = Path(os.path.join(root, file))
            if file.endswith(".json"):
                yield extract_json_as_dict(full_file_path), full_file_path
            if file.endswith(".xml"):
                yield extract_xml_as_dict(full_file_path), full_file_path
