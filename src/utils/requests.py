import requests
import json
import logging


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


def download_file(url, path):
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    f.write(chunk)
        logging.info(f"File downloaded successfully to {path}")
        return path
    except requests.exceptions.RequestException as e:
        err_msg = f"Error fetching data: {e}"
        logging.error(err_msg)
        raise Exception(err_msg)
