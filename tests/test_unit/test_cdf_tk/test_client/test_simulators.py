import pytest

from cognite_toolkit._cdf_tk.client import ToolkitClient


class TestSimulatorsAPI:
    def test_list_rejects_limit_above_max(self, toolkit_client: ToolkitClient) -> None:
        with pytest.raises(ValueError, match="Limit must be between 1 and 1000"):
            toolkit_client.simulators.list(limit=1001)
