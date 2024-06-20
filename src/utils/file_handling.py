import os


def ensure_path(path):
    """
    Ensures the path exists and creates it if it doesnt exist.
    """
    if not os.path.exists(path):
        os.makedirs(path)
