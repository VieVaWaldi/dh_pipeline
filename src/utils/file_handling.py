import os
import zipfile
import logging


def ensure_path(path):
    """
    Ensures the path exists and creates it if it doesnt exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)


def unpack_and_remove_zip(zip_path: str):
    """
    Unpacks a zip file given a path and places all contents in the same directory.
    Returns the folder
    """
    save_directory = os.path.dirname(zip_path)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(save_directory)
        logging.info("Extracted all files to: %s", save_directory)

    os.remove(zip_path)
    logging.info("Removed the zip file: %s", zip_path)
