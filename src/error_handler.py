import logging
import sys
import traceback
from functools import wraps
from typing import Any, Callable

from google.ads.googleads.errors import GoogleAdsException

from src import notification


def handle_google_ads_exception(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except GoogleAdsException as ex:
            logging.error(
                f'Request with ID "{ex.request_id}" failed with status '
                f'"{ex.error.code().name}" and includes the following errors:'
            )
            for error in ex.failure.errors:
                logging.error(f'\tError with message "{error.message}".')
                if error.location:
                    for field_path_element in error.location.field_path_elements:
                        logging.error(f"\t\tOn field: {field_path_element.field_name}")
            sys.exit(1)

    return wrapper


def handle_global_exception(func: Callable[..., Any]) -> Callable[..., Any]:
    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except (Exception, BaseException) as ex:
            logging.error(f"An unexpected error occurred: {ex}")
            error_message = traceback.format_exc()
            notification.send_message(
                f"An unexpected error occurred in Google Ads process:\n{error_message}"
            )
            sys.exit(1)

    return wrapper
