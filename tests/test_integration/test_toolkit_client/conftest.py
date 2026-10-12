import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceRequest


@pytest.fixture(scope="session")
def dev_space(dev_cluster_client: ToolkitClient) -> str:
    """Fixture to create a space for the tests."""
    space_name = "toolkit_test_space"
    dev_cluster_client.tool.spaces.create([SpaceRequest(space=space_name)])
    return space_name
