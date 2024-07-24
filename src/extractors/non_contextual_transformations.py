import logging
import os
import re
import time
from pathlib import Path

from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from utils.file_handling import ensure_path_exists
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


class CordisPDFDownloader:
    def __init__(self, download_path: Path):
        self.download_path = download_path
        self.driver = self._setup_driver()
        logging.info(f"Setup CordisPDFDownloader for path {self.download_path}.")

    def _setup_driver(self):
        chrome_options = Options()
        chrome_options.add_argument("--headless=new")
        chrome_options.add_experimental_option(
            "prefs",
            {
                "download.default_directory": str(self.download_path),
                "download.prompt_for_download": False,
                "download.directory_upgrade": True,
                "safebrowsing.enabled": True,
                "plugins.always_open_pdf_externally": True,
            },
        )
        return webdriver.Chrome(options=chrome_options)

    def download_pdf(self, url: str) -> bool:
        if get_base_url(url) != "europa.eu":
            log_and_raise_exception(
                "Trying to download a non europa.eu cordis pdf file."
            )
        try:
            self.driver.get(url)
            return self._wait_for_download()
        except Exception as e:
            print(f"Error downloading PDF from {url}: {str(e)}")
            return False

    def _wait_for_download(self, timeout: int = 90) -> bool:
        start_time = time.time()
        while time.time() - start_time < timeout:
            downloading_files = [
                f
                for f in os.listdir(self.download_path)
                if f.endswith(".crdownload")
                or f.endswith(".tmp")
                or f.startswith(".com.google.Chrome")
            ]
            if not downloading_files:
                return True
            time.sleep(1)
        self._cleanup_incomplete_files()
        return False

    def _cleanup_incomplete_files(self) -> None:
        for filename in os.listdir(self.download_path):
            if (
                filename.endswith(".crdownload")
                or filename.endswith(".tmp")
                or filename.startswith(".com.google.Chrome")
            ):
                file_path = os.path.join(self.download_path, filename)
                try:
                    os.remove(file_path)
                except Exception as e:
                    print(f"Error deleting {filename}: {str(e)}")

    def close(self):
        if self.driver:
            self.driver.quit()


if __name__ == "__main__":
    save_path = Path(
        "/Users/wehrenberger/Code/DIGICHer/DIGICHer_Pipeline/data/pile/rmme"
    )
    ensure_path_exists(save_path)
    downloader = CordisPDFDownloader(save_path)

    try:
        result = downloader.download_pdf(
            "https://ec.europa.eu/research/participants/documents/downloadPublic?documentIds=080166e5e9e8c93d&appId=PPGMS"
        )
        print(f"Download successful: {result}")
    finally:
        downloader.close()
