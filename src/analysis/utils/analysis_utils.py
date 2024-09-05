from pathlib import Path
from typing import Any


bad_ones = ["\n", "\t", "\r", ";"]


def clean_value(value: Any) -> str:
    if value is None:
        return ""
    for bad in bad_ones:
        value = str(value).replace(bad, " ")
    return value


def is_cordis_only_project_flag(is_flag: bool, path: Path):
    return is_flag and "cordis" in str(path) and "project" not in str(path)
