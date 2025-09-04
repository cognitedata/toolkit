import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig


class TestToolkitClientConfig:
    def test_create_app_url_raise_on_empty(self, toolkit_config: ToolkitClientConfig) -> None:
        with pytest.raises(ValueError, match="Endpoint must be a non-empty string"):
            toolkit_config.create_app_url("")
