import time
from collections.abc import Callable
from pathlib import Path
from typing import Any, TypeVar
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO

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


def load_local_yaml(local_yaml_content: str, io: ResourceIO) -> dict[str, Any]:
    """Load a local YAML file and return the first resource.

    This is typically used in tests to ensure that a resource is not redeployed when it is unchanged.
    By loading the resource from a local YAML file, we get the same representation of th resource as
    cdf deploy would use. Note that .load_resoruce_file sometimes do custom parsing based on the resource type.
    """
    local_yaml = MagicMock(spec=Path)
    local_yaml.read_text.return_value = local_yaml_content
    raw_data_list = io.load_resource_file(local_yaml)
    assert len(raw_data_list) == 1, f"Expected 1 resource in the YAML file, but found {len(raw_data_list)}"
    return raw_data_list[0]
