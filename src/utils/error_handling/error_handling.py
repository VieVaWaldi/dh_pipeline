import logging


def log_and_raise_exception(message: str, exception: Exception = None):
    """
    Logs the message and raises a standard exception.
    """
    if exception:
        logging.exception(f"{message}: {str(exception)}")
    else:
        logging.error(message)
    raise Exception()
