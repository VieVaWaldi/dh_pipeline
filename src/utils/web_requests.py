import json
import logging
import os
from typing import Dict
from urllib.parse import urlparse

import requests

from utils.logger import log_and_raise_exception


def make_get_request(url: str, params: Dict) -> Dict:
    """
    Makes a get request given a url and params.
    Returns a json Dict or raises an exception.
    """
    try:
        response = requests.get(url, params=params, timeout=60)
        if response.status_code == 200:
            logging.info(
                "GET Request status: %s",
                json.dumps(response.json(), separators=(",", ":")),
            )
            return response.json()
        else:
            return log_and_raise_exception(
                f"Error fetching data: {response.status_code}, {response.text}"
            )
    except Exception as e:
        return log_and_raise_exception(f"Error fetching data: {e}")


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
                json.dumps(response.json(), separators=(",", ":")),
            )
            return response.json()
        else:
            return log_and_raise_exception(
                f"Error fetching data: {response.status_code}, {response.text}"
            )
    except Exception as e:
        return log_and_raise_exception(f"Error fetching data: {e}")


def download_file(url: str, save_path: str) -> str:
    """
    Downloads a file given an url and stores it under the specified path.
    Expects path to be valid. Returns path of saved file.
    """

    filename = os.path.basename(url)
    file_path = os.path.join(save_path, filename)

    try:
        response = requests.get(url, timeout=60)
        response.raise_for_status()

        with open(file_path, "wb") as file:
            file.write(response.content)
            logging.info("File downloaded successfully to %s", file_path)
        return file_path
    except Exception as e:
        return log_and_raise_exception(f"Error fetching data: {e}")


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
