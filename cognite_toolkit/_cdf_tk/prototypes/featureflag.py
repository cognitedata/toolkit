import os

from rich import print


def enabled(flag: str) -> bool:
    """
    Check if a feature flag is enabled.

    Args:
        flag (str): The feature flag to check.

    Returns:
        bool: True if the feature flag is enabled, False otherwise.
    """
    if os.environ.get(flag, "false").lower() == "true":
        if enabled("FF_PRINT_FLAGS"):
            print(f"[yellow]Feature flag {flag} is enabled.[/]")
        return True
    return False
