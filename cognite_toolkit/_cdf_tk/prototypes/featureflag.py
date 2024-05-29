import os
from functools import lru_cache

import dotenv


@lru_cache(maxsize=128)
def enabled(flag: str) -> bool:
    """
    Check if a feature flag is enabled.

    Args:
        flag (str): The feature flag to check.

    Returns:
        bool: True if the feature flag is enabled, False otherwise.
    """
    dotenv.load_dotenv()
    if os.environ.get(flag, "false").lower() == "true":
        print(f"Feature flag {flag} is enabled.")
        return True
    return False
