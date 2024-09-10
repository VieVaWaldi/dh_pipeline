from enum import Enum, auto
from typing import Dict, Any


class Tables(Enum):
    PROJECTS = "projects"
    PUBLICATIONS = "publications"
    PEOPLE = "people"
    INSTITUTIONS = "institutions"
    TOPICS = "topics"
    FUNDING_PROGRAMMES = "funding_programmes"
    DOIS = "dois"
    WEBLINKS = auto()


def check_source_schema(source_schema: Dict[str, Any]):
    """
    Check that all entries have table, column and type.
    Check that table and column are from enums.
    """
    pass


def traverse_dictionary_update_table_dict(
    document: Dict[str, Any],
    table_dict: Dict[str, Any],
    source_schema: Dict[str, Any],
    parent_key: str = "",
    list_idx: int = None,
):
    for key, value in document.items():
        current_key = f"{parent_key}.{key}" if parent_key else key

        if isinstance(value, dict):
            traverse_dictionary_update_table_dict(
                value, table_dict, source_schema, current_key
            )

        elif isinstance(value, list):
            for idx, item in enumerate(value):
                if isinstance(item, dict):
                    traverse_dictionary_update_table_dict(
                        item, table_dict, source_schema, f"{current_key}[_]", idx
                    )
                else:
                    update_table_schema(
                        table_dict, source_schema, f"{current_key}[_]", str(item), idx
                    )
        else:
            update_table_schema(
                table_dict, source_schema, current_key, str(value), list_idx
            )


def update_table_schema(
    table_dict: Dict[Any, str],
    source_schema: Dict[Any, str],
    full_key: str,
    value: str,
    list_idx: int = None,
):
    cell = source_schema.get(full_key)
    if cell is None:
        return

    table_name = cell.get("table")

    if value is not None:
        print(value)

    if list_idx is None:
        table_dict[table_name][full_key] = value
    elif list_idx == 0:
        table_dict[table_name][full_key] = [value]
    else:
        table_dict[table_name][full_key].append(value)
