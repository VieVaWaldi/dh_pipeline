import re
import unicodedata

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

    name = name.replace("!", "").replace("|", "").replace("-", " ").replace(".", " ")
    name = unicodedata.normalize("NFKD", name).encode("ASCII", "ignore").decode("utf-8")
    name = name.lower()
    
    pattern = r"\b(" + "|".join(noise_words) + r")\b"
    name = re.sub(pattern, "", name)

    name = (
        name.replace("university", "univ")
        .replace("instituto", "inst")
        .replace("institute", "inst")
        .replace("laboratory", "lab")
        .replace("laboratoire", "lab")
        .replace("foundation", "found")
        .replace("national", "natl")
        .replace("international", "intl")
        .replace("research", "res")
        .replace("technology", "tech")
        .replace("technolog", "tech")
        .replace("sciences", "sci")
        .replace("center", "ctr")
        .replace("centre", "ctr")
        .replace("departement", "dept")
        .replace("department", "dept")
    )

    name = re.sub(r"\s+", " ", name).strip()

    return name
