import logging


def log_and_raise_exception(message: str, exception: Exception = None) -> None:
    """
    Logs the message and raises a standard exception.
    """
    if exception:
        logging.error(f"{message} {str(exception)}")
    else:
        logging.error(message)
    raise Exception(message)
