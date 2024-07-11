import logging
import os
import re
import time

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from utils.logger import log_and_raise_exception
from utils.web_requests import get_base_url


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


def download_and_save_cordis_pdfs(url: str, save_path: str) -> None:
    """
    Specifically downloads files from europa.eu, which forces a redirect to the download and just sucks in general.
    So we use selenium and monitor the download directory.
    """

    if get_base_url(url) != "europa.eu":
        log_and_raise_exception("Trying to download a non europa.eu cordis pdf file.")

    CHROME_OPTIONS = Options()
    CHROME_OPTIONS.add_argument("--headless=new")
    CHROME_OPTIONS.add_experimental_option(
        "prefs",
        {
            "download.default_directory": save_path,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "plugins.always_open_pdf_externally": True,
        },
    )
    DRIVER = webdriver.Chrome(options=CHROME_OPTIONS)

    try:
        DRIVER.get(url)
        if not wait_for_download(save_path):
            logging.info("Cordis file couldn't be downloaded.")
    except:
        logging.info("Cordis file couldn't be downloaded.")


def wait_for_download(directory: str, timeout: int = 90) -> bool:
    """
    Waits until .crdownload, .tmp or .com files are not in the directory anymore.
    Deletes these files on timeout.
    """
    initial_files = set(os.listdir(directory))
    start_time = time.time()
    while time.time() - start_time < timeout:
        downloading_files = [
            f
            for f in os.listdir(directory)
            if f.endswith(".crdownload")
            or f.endswith(".tmp")
            or f.startswith(".com.google.Chrome")
        ]
        if downloading_files:
            time.sleep(1)
            continue

        new_files = set(os.listdir(directory)) - initial_files
        if new_files:
            logging.info(f"Cordis Download completed: {list(new_files)[-1]}")
            return True

        time.sleep(1)

    cleanup_incomplete_files(directory)
    return False


def cleanup_incomplete_files(directory: str) -> None:
    """
    Deletes .crdownload, .tmp, and .com.google.Chrome files in the specified directory.
    """
    for filename in os.listdir(directory):
        if (
            filename.endswith(".crdownload")
            or filename.endswith(".tmp")
            or filename.startswith(".com.google.Chrome")
        ):
            file_path = os.path.join(directory, filename)
            try:
                os.remove(file_path)
            except Exception as e:
                print(f"Error deleting {filename}: {str(e)}")


if __name__ == "__main__":
    download_and_save_cordis_pdfs(
        "https://ec.europa.eu/research/participants/documents/downloadPublic?documentIds=080166e5e9e8c93d&appId=PPGMS",
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/rmme",
    )
