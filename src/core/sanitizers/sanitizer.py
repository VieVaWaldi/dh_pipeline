from datetime import datetime
from typing import Optional, Any

""" Parse Primitives """


def parse_bool(value: Any) -> Optional[bool]:
    """Use for all booleans"""
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.lower() == "true"
    return None


def parse_number(value: Optional[str]) -> Optional[int]:
    """Use for all decimals"""
    if not value:
        return None
    try:
        return int(value)
    except ValueError:
        return None


def parse_float(value: Any) -> Optional[float]:
    """Use for all floats"""
    if value is not None:
        try:
            return float(value)
        except (ValueError, TypeError):
            return None
    return None


""" Parse String
DOIs and acronyms should generally NOT be sanitized.
"""


def _ensure_string(value: Any) -> Optional[str]:
    """Convert any value to string, or return None if conversion fails."""
    if value is None:
        return None
    try:
        return str(value)
    except (ValueError, TypeError):
        return None


def parse_string(value: Optional[str]) -> Optional[str]:
    """
    Use for all other string than dont fit one of the next methods.
    Trims whitespace and normalizes internal spaces.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None
    clean_str = " ".join(filter(None, value.strip().split()))
    return clean_str or None


def parse_names_and_identifiers(value: Optional[str]) -> Optional[str]:
    """
    Use e.g. for author names, institution names, department names.

    Preserves hyphens, apostrophes, and periods in names.
    Trims whitespace and normalizes internal spaces.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None

    clean_str = value.strip()
    clean_str = clean_str.replace("\t", " ").replace("\r", "")
    clean_str = " ".join(filter(None, clean_str.split()))

    return clean_str or None


def parse_titles_and_labels(value: Optional[str]) -> Optional[str]:
    """
    Use e.g. for topic names, scientific domains, funding programme names,
    project titles, publication titles.

    Preserves hyphens, colons, and other punctuation relevant to titles.
    Replaces newlines/tabs with spaces and normalizes internal spacing.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None

    clean_str = value.strip()
    clean_str = (
        clean_str.replace("\n", " ")
        .replace("\t", " ")
        .replace("\r", "")
        .replace("–", "-")
        .replace("—", "-")
    )
    clean_str = " ".join(filter(None, clean_str.split()))

    return clean_str or None


def parse_content(value: Optional[str]) -> Optional[str]:
    """
    Use e.g. for abstracts, full text, and other multi-paragraph content.

    Preserves paragraph structure (newlines) while normalizing spacing within paragraphs.
    Removes excessive newlines and normalizes to single newlines between paragraphs.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None

    clean_str = value.strip()
    clean_str = clean_str.replace("\r", "").replace("\x00", "")

    paragraphs = clean_str.split("\n")
    cleaned_paragraphs = []

    for paragraph in paragraphs:
        cleaned = paragraph.strip().replace("\t", " ")
        cleaned = " ".join(filter(None, cleaned.split()))
        if cleaned:
            cleaned_paragraphs.append(cleaned)

    result = "\n".join(cleaned_paragraphs)
    return result or None


def parse_web_resources(value: Optional[str]) -> Optional[str]:
    """
    Use for URLs and other web resources.

    Minimal processing - only trims whitespace and removes carriage returns.
    Returns None if string ends up being empty.
    """
    value = _ensure_string(value)
    if not value:
        return None

    clean_str = value.strip().replace("\r", "")
    clean_str = clean_str.replace("\n", "").replace("\t", "")

    return clean_str or None


""" Parse Date """


def parse_date(date_str: Optional[str]) -> Optional[datetime]:
    """Parse date string into datetime object."""
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str.replace("Z", "+00:00"))
    except (ValueError, TypeError):
        return None


""" Parse Special Cases """


def parse_geolocation(geolocation: str, swap_lat_lon: bool) -> Optional[list]:
    """
    Parse geolocation string and return as [lon, lat] array.
    Returns None if coordinates are invalid.
    Cordis geolocations with (brackets) are [lat, lon] in the raw data.
    """
    if not geolocation:
        return None

    cleaned = geolocation.replace("(", "").replace(")", "")
    try:
        lat, lon = map(lambda x: float(x.strip()), cleaned.split(","))
        if swap_lat_lon and not geolocation.startswith("("):
            lat, lon = lon, lat
        if lat < -90 or lat > 90 or lon < -180 or lon > 180:
            return None

        return [lat, lon]
    except (ValueError, TypeError):
        return None
