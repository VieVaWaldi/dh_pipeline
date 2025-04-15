import concurrent.futures
import datetime
import multiprocessing
from pathlib import Path
from typing import List

import PyPDF2

from enrichment.llm.deepseek import get_title_from_text


def pdf_to_text(pdf_path: Path) -> List[str]:
    with open(pdf_path, "rb") as file:
        pdf_reader = PyPDF2.PdfReader(file)
        num_pages = len(pdf_reader.pages)

        # with open(output_path, "w", encoding="utf-8") as out_file:

        # pdf_str = ""
        pdf_str = ""
        for page_num in range(num_pages):
            page = pdf_reader.pages[page_num]

            text = page.extract_text()

            pdf_str += f"[Page {page_num + 1}]\n"
            # pdf_str += text
            # pdf_str += "\n"
            pdf_str += text
        return pdf_str


def pdf_to_text_concurrent():
    pass


def clean(text: str):
    return text.replace("\n", " ").replace("\r", " ").replace("\t", " ")


if __name__ == "__main__":
    file_dir = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/cordis_culturalORheritage/project-rcn-225907_en/attachments"
    )

    start_time = datetime.datetime.now()
    files = []
    results = []
    for file in file_dir.iterdir():
        if file.is_file() and file.name.endswith(".pdf"):
            files.append(file)
            # results.append(pdf_to_text(file))

    with concurrent.futures.ProcessPoolExecutor(
        max_workers=multiprocessing.cpu_count()
    ) as executor:
        results = list(executor.map(pdf_to_text, files))

    print(f"Took {datetime.datetime.now() - start_time} seconds to process files")
    start_time = datetime.datetime.now()

    contents = []
    titles = []
    for file, content in zip(files, results):
        print(f"{file.name:25} | {clean(content).strip()[:100]}")
        # contents.append(clean(content).strip()[:2000])
        titles.append(get_title_from_text(clean(content).strip()[:2000]))

    # titles = get_title_from_text_concurrent(contents)
    for t in titles:
        print(t)
    print(
        f"Took {datetime.datetime.now() - start_time} seconds to get titles from deepseek"
    )

    # 21 files

    # Time OCR pdf
    # serial, 14 s
    # multi, 3 s

    # first 2000 characters for 21 files

    # Time Deep seek batch
    # serial,
    # batch, 2 minutes
