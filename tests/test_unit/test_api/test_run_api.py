from __future__ import annotations

from cognite.client.data_classes import Transformation

from cognite_toolkit._api import CogniteToolkit
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestRunAPI:
    def test_run_transformation(
        self,
        cognite_toolkit: CogniteToolkit,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_mock: CDFToolConfig,
    ) -> None:
        transformation = Transformation(
            name="Test transformation",
            external_id="test",
            query="SELECT * FROM timeseries",
        )
        toolkit_client_approval.append(Transformation, transformation)

        result = cognite_toolkit.run.transformation("test")

        assert result is True
