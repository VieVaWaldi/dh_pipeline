import re


def trim_excessive_whitespace(file_content: str) -> str:
    """
    1. Replaces multiple newlines with a single newline
    2. Replace multiple spaces with a single space, but not newlines
    3. Ensure there is a newline after each tag for readability
    """
    trimmed_content = re.sub(r"\n\s*\n", "\n", file_content)
    trimmed_content = re.sub(r"[ \t]+", " ", trimmed_content)
    trimmed_content = re.sub(r"(>)(<)", r"\1\n\2", trimmed_content)
    return trimmed_content
