import pytest
from cognite.client.data_classes.data_modeling import SpaceApply

from cognite_toolkit._cdf_tk.client import ToolkitClient


@pytest.fixture(scope="session")
def dev_space(dev_cluster_client: ToolkitClient) -> str:
    """Fixture to create a space for the tests."""
    space_name = "toolkit_test_space"
    dev_cluster_client.data_modeling.spaces.apply(SpaceApply(space=space_name))
    return space_name
