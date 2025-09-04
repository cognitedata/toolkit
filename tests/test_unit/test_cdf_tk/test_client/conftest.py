import pytest
from cognite.client import global_config

TOKEN_URL = "https://test.com/token"


@pytest.fixture
def max_retries_2():
    old = global_config.max_retries
    global_config.max_retries = 2
    yield
    global_config.max_retries = old
