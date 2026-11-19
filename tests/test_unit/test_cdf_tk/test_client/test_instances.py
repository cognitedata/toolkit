import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import NodeId


@pytest.mark.usefixtures("disable_gzip")
class TestInstancesAPI:
    def test_inspect(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/models/instances/inspect")).mock(
            return_value=httpx2.Response(200, json={"items": []})
        )

        toolkit_client.instances.inspect([NodeId(space="my_space", external_id="my_node")], all_versions=True)

        assert json.loads(route.calls[0].request.content) == {
            "items": [{"instanceType": "node", "space": "my_space", "externalId": "my_node"}],
            "inspectionOperations": {
                "involvedViews": {"allVersions": True},
                "involvedContainers": {"allVersions": True},
            },
        }
