from typing import Any


bad_ones = ["\n", "\t", "\r", ";"]


def clean_value(value: Any) -> str:
    if value is None:
        return ""
    for bad in bad_ones:
        value = str(value).replace(bad, " ")
    return value
