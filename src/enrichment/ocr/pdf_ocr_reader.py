import logging
import re
import unicodedata
from pathlib import Path
from typing import Optional

import PyPDF2

from core.sanitizers.sanitizer import parse_content

logging.getLogger("PyPDF2").setLevel(logging.ERROR)


def pdf_to_text(pdf_path: Path) -> Optional[str]:
    try:
        full_text = ""
        with open(pdf_path, "rb") as file:
            reader = PyPDF2.PdfReader(file)
            for page_num in range(len(reader.pages)):
                page = reader.pages[page_num]
                text = page.extract_text()
                text = sanitize_pdf_text(text)
                text = parse_content(text)
                if text:
                    full_text += text + "\n\n"  # Page breaks
        return full_text
    except Exception as e:
        logging.warning(f"Failed to OCR pdf from: {pdf_path},\n{e}")
        return None


def sanitize_pdf_text(text: Optional[str]) -> Optional[str]:
    """
    Sanitizes text extracted from PDFs to ensure clean UTF-8 output.

    Handles illegal characters, non-standard encodings, and special symbols
    commonly found in academic publications. Preserves meaningful content
    while removing problematic characters that could cause encoding errors.
    """
    if not text:
        return None

    # Normalize Unicode characters (e.g., combining diacritical marks)
    text = unicodedata.normalize("NFKD", text)

    # Remove control characters but preserve newlines and tabs
    text = re.sub(r"[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]", "", text)

    # Handle common problematic characters in PDFs
    replacements = {
        "\ufeff": "",  # BOM
        "\u2028": "\n",  # Line separator
        "\u2029": "\n",  # Paragraph separator
        "\xad": "-",  # Soft hyphen
        "–": "-",  # En dash
        "—": "-",  # Em das
        '"': '"',
        "…": "...",
    }

    for old, new in replacements.items():
        text = text.replace(old, new)

    # Filter out any truly problematic characters while keeping valid UTF-8
    text = "".join(c for c in text if c.isprintable() or c in "\n\t")

    # Remove excessive spacing
    text = re.sub(r"\s{2,}", " ", text)

    return text.strip() or None


if __name__ == "__main__":
    file_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/arxiv_allCOLONcomputingPLUSANDPLUSLBallCOLONhumanitiesPLUSORPLUSallCOLONheritageRB/last_start_46000/2024-08-07T08:19:44Z_A_Logical_Fallacy_Informed_Framework_for/paper_2024-08-07T08:19:44Z_A_Logical_Fallacy_Informed_Framework_for.pdf"
    )
    print(pdf_to_text(file_path))

# def pdf_to_text2(pdf_path: Path) -> Optional[str]:
#     try:
#         full_text = ""
#         # Open the PDF
#         with fitz.open(pdf_path) as doc:
#             for page_num in range(len(doc)):
#                 page = doc[page_num]
#
#                 # Extract text with the "text" method (better for academic papers)
#                 # blocks=True maintains better paragraph structure
#                 text = page.get_text("text", sort=True)
#
#                 text = sanitize_pdf_text(text)
#                 # text = fix_academic_paper_spacing(text)
#                 text = parse_content(text)
#
#                 if text:
#                     full_text += text + "\n\n"  # Page breaks
#         return full_text
#     except Exception as e:
#         logging.warning(f"Failed to extract pdf from: {pdf_path},\n{e}")
#         return None

# if __name__ == "__main__":
#     file_dir = Path(
#         "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/cordis_culturalORheritage/project-rcn-225907_en/attachments"
#     )

#     start_time = datetime.datetime.now()
#     files = []
#     results = []
#     for file in file_dir.iterdir():
#         if file.is_file() and file.name.endswith(".pdf"):
#             files.append(file)
#             # results.append(pdf_to_text(file))

#     with concurrent.futures.ProcessPoolExecutor(
#         max_workers=multiprocessing.cpu_count()
#     ) as executor:
#         results = list(executor.map(pdf_to_text, files))

#     print(f"Took {datetime.datetime.now() - start_time} seconds to process files")
#     start_time = datetime.datetime.now()

#     contents = []
#     titles = []
#     for file, content in zip(files, results):
#         print(f"{file.name:25} | {clean(content).strip()[:100]}")
#         # contents.append(clean(content).strip()[:2000])
#         titles.append(get_title_from_text(clean(content).strip()[:2000]))

#     # titles = get_title_from_text_concurrent(contents)
#     for t in titles:
#         print(t)
#     print(
#         f"Took {datetime.datetime.now() - start_time} seconds to get titles from deepseek"
#     )

#     # 21 files

#     # Time OCR pdf
#     # serial, 14 s
#     # multi, 3 s

#     # first 2000 characters for 21 files

#     # Time Deep seek batch
#     # serial,
#     # batch, 2 minutes
