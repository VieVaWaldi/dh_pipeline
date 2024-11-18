from datetime import datetime
from typing import Optional


class DataSanitizer:
    @staticmethod
    def clean_string(value: Optional[str]) -> Optional[str]:
        if not value:
            return None
        return value.strip().replace("\n", " ").replace("\r", "")

    @staticmethod
    def clean_date(date_str: Optional[str]) -> Optional[datetime]:
        if not date_str:
            return None
        try:
            return datetime.fromisoformat(date_str.rstrip("Z"))
        except ValueError:
            return None

    @staticmethod
    def clean_number(value: Optional[str]) -> Optional[int]:
        if not value:
            return None
        try:
            return int(value)
        except ValueError:
            return None

    @staticmethod
    def clean_name(name: Optional[str]) -> Optional[str]:
        if not name:
            return None
        # Remove extra spaces, standardize separators
        name = " ".join(name.split())
        # Additional name-specific cleaning...
        return name
