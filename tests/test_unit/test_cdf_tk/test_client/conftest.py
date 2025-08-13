import pytest
from cognite.client import global_config
from cognite.client.credentials import Token

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig

BASE_URL = "http://blabla.cognitedata.com"
TOKEN_URL = "https://test.com/token"


@pytest.fixture
def toolkit_config():
    return ToolkitClientConfig(
        client_name="test-client",
        project="test-project",
        base_url=BASE_URL,
        max_workers=1,
        timeout=10,
        credentials=Token("abc"),
    )


@pytest.fixture
def max_retries_2():
    old = global_config.max_retries
    global_config.max_retries = 2
    yield
    global_config.max_retries = old
