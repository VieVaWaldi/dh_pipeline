import os
import requests
import json
import logging

from utils.file_handling import ensure_path


def make_request(url, params):
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            logging.info(
                f"Request status: {json.dumps(response.json(), separators=(',', ':'))}",
            )
            return response.json()
        else:
            err_msg = f"Error fetching data: {response.status_code}, {response.text}"
            logging.error(err_msg)
            raise Exception(err_msg)
    except Exception as e:
        err_msg = f"Error fetching data: {e}"
        logging.error(err_msg)
        raise Exception(err_msg)


def download_file(url, save_path):
    ensure_path(save_path)

    filename = os.path.basename(url)
    save_path = os.path.join(save_path, filename)

    try:
        response = requests.get(url)
        response.raise_for_status()

        with open(save_path, "wb") as file:
            file.write(response.content)

            logging.info(f"File downloaded successfully to {save_path}")
        return save_path
    except Exception as e:
        err_msg = f"Error fetching data: {e}"
        logging.error(err_msg)
        raise Exception(err_msg)
