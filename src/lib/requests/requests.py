import json
import logging
import time
from pathlib import Path
from typing import Dict
from urllib.parse import urlparse

import requests
from requests import Response

from lib.requests.retry_request import retry_on_failure
from lib.sanitizers.parse_text import flatten_string
from utils.error_handling.error_handling import log_and_raise_exception

MAX_RESPONSE_LOG_LENGTH = 1024


@retry_on_failure()
def make_get_request(
    url: str, params: Dict = None, header: Dict = None, expect_json: bool = True
) -> dict | Response:
    """
    Makes a get request given an url and optional params and header.
    Returns a json Dict for expect_json else text or raises an exception.
    """
    try:
        response = requests.get(url, params=params, headers=header, timeout=60)
        if response.status_code != 200:
            log_and_raise_exception(
                f"Error fetching data: {response.status_code}, {response.text[:MAX_RESPONSE_LOG_LENGTH]}"
            )

        logging.info(
            "GET Request status: %s",
            flatten_string(response.text[:MAX_RESPONSE_LOG_LENGTH]),
        )
        return response.json() if expect_json else response
    except Exception as e:
        log_and_raise_exception(f"Error fetching data", e)


def make_delete_request(url: str, params: Dict) -> Dict:
    """
    Makes a delete request given a url and params.
    Returns a json Dict or raises an exception.
    """
    try:
        response = requests.delete(url, params=params, timeout=60)
        if response.status_code == 200:
            logging.info(
                "DELETE Request status: %s",
                json.dumps(response.json(), separators=(",", ":"))[
                    :MAX_RESPONSE_LOG_LENGTH
                ],
            )
            return response.json()
        else:
            log_and_raise_exception(
                f"Error fetching data: {response.status_code}, {response.text[:MAX_RESPONSE_LOG_LENGTH]}"
            )
    except Exception as e:
        log_and_raise_exception(f"Error fetching data", e)


def download_file(url: str, save_path: Path) -> Path:
    """
    Downloads a file given an url and stores it under the specified path.
    Expects path to be valid. Returns path of saved file.
    """

    file_path = save_path / "tmp.zip"

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        with open(file_path, "wb") as file:
            file.write(response.content)
            logging.info(f"File downloaded successfully to {file_path}")
        return file_path
    except Exception as e:
        log_and_raise_exception(f"Error fetching data", e)


def get_base_url(url: str):
    """
    Returns the subdomain and toplevel domain.
    Eg returns europa.eu from https://ec.europa.eu/research/.
    """
    parsed_url = urlparse(url)
    parts = parsed_url.netloc.split(".")
    if len(parts) >= 2:
        base_url = ".".join(parts[-2:])
    else:
        base_url = parsed_url.netloc
    return base_url
