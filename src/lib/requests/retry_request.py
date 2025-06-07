import logging
import time


def retry_on_failure(max_retries=3, initial_delay=10, power_base=4, enable_retry=True):
    def decorator(func):
        def wrapper(*args, **kwargs):
            call_enable_retry = kwargs.pop("enable_retry", enable_retry)
            if not call_enable_retry:
                return func(*args, **kwargs)

            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if attempt == max_retries:
                        raise e
                    delay = initial_delay * (power_base**attempt)
                    logging.info(
                        f"Request failed (attempt {attempt+1}/{max_retries}). Retrying in {delay} seconds..."
                    )
                    time.sleep(delay)

        return wrapper

    return decorator
