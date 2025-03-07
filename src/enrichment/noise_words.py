import unicodedata
import re

noise_words = [
    "the",
    "and",
    "of",
    "for",
    "a",
    "an",
    "in",
    "on",
    "at",
    "by",
    "with",
    "ltd",
    "llc",
    "inc",
    "corporation",
    "corp",
    "co",
    "limited",
    "gmbh",
    "sa",
    "s.a.",
    "s.r.l.",
    "spa",
    "plc",
    "pvt",
    "bv",
    "nv",
    "ag",
    "ab",
]


def normalize_institution_name(name: str) -> str:
    """
    Normalize institution name for better matching with OpenAlex.

    1. Remove diacritics and special characters
    2. Convert to lowercase
    3. Remove common words and abbreviations that might cause noise
    4. Standardize variations of university, institute, etc.
    """
    if not name:
        return ""

    # Remove special characters
    name = name.replace("!", "").replace("|", "").replace("-", " ").replace(".", " ")
    # Remove diacritics
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("utf-8")
    # Convert to lowercase
    name = name.lower()

    # Create a pattern to match these words as whole words only
    pattern = r"\b(" + "|".join(noise_words) + r")\b"
    name = re.sub(pattern, "", name)

    # Standardize common institution type variations
    name = name.replace("university", "univ")
    name = name.replace("instituto", "inst")
    name = name.replace("institute", "inst")
    name = name.replace("laboratory", "lab")
    name = name.replace("laboratoire", "lab")
    name = name.replace("foundation", "found")
    name = name.replace("national", "natl")
    name = name.replace("international", "intl")
    name = name.replace("research", "res")
    name = name.replace("technology", "tech")
    name = name.replace("technolog", "tech")
    name = name.replace("sciences", "sci")
    name = name.replace("center", "ctr")
    name = name.replace("centre", "ctr")
    name = name.replace("departement", "dept")
    name = name.replace("department", "dept")

    name = re.sub(r"\s+", " ", name).strip()

    return name
