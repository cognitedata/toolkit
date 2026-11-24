import time
import warnings
from collections.abc import Callable

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError


def retry_api_call(
    api_call: Callable[[], None],
    timeout_seconds: float = 60.0,
    poll_interval: float = 1.0,
) -> None:
    """Retry an action until it succeeds or a timeout is reached.

    Args:
        api_call: The action to retry.
        timeout_seconds: The maximum time to wait for the action to succeed.
        poll_interval: The time to wait between retries.

    Returns:
        None
    """
    start_time = time.monotonic()
    while (time.monotonic() - start_time) < timeout_seconds:
        try:
            api_call()
            return
        except ToolkitAPIError:
            pass
        time.sleep(poll_interval)
    api_call()  # Final attempt, will raise if it fails


def best_effort_cleanup(*actions: Callable[[], object]) -> None:
    """Run every cleanup action, turning API errors into warnings.

    Cleanup runs in ``finally`` blocks. If it raises, it hides the error that made the test fail,
    and a failed delete must not stop the remaining resources from being cleaned up.
    """
    for action in actions:
        try:
            action()
        except ToolkitAPIError as e:
            warnings.warn(f"Smoke test cleanup failed: {e!s}", stacklevel=2)
