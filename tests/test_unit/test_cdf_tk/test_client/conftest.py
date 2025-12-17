import pytest
from cognite.client import global_config

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig

TOKEN_URL = "https://test.com/token"


@pytest.fixture
def max_retries_2():
    old = global_config.max_retries
    global_config.max_retries = 2
    yield
    global_config.max_retries = old


@pytest.fixture(scope="session")
def toolkit_client(toolkit_config: ToolkitClientConfig) -> ToolkitClient:
    return ToolkitClient(toolkit_config)
