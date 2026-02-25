import time
from collections.abc import Callable
from typing import TypeVar

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError

T = TypeVar("T")


def retry_on_deadlock(func: Callable[[], T], delay: float = 10.0) -> T:
    """Execute a function with retry logic for deadlock errors.

    Args:
        func: The function to execute.
        delay: The delay in seconds before retrying after a deadlock.

    Returns:
        The result of the function call.

    Raises:
        ToolkitAPIError: If the error is not a deadlock or retry also fails.
    """
    try:
        return func()
    except ToolkitAPIError as e:
        if e.code == 409 and "deadlock" in e.message.lower():
            time.sleep(delay)
            return func()
        raise
