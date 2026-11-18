import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId

_UNIT = {
    "externalId": "temperature:deg_c",
    "name": "DEG_C",
    "longName": "degree Celsius",
    "symbol": "°C",
    "aliasNames": ["C"],
    "quantity": "Temperature",
    "conversion": {"multiplier": 1.0, "offset": 273.15},
}


@pytest.mark.usefixtures("disable_gzip")
class TestUnitsAPI:
    def test_retrieve(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        route = respx_mock.post(toolkit_config.create_api_url("/units/byids")).mock(
            return_value=httpx2.Response(200, json={"items": [_UNIT]})
        )

        toolkit_client.units.retrieve([ExternalId(external_id="temperature:deg_c")], ignore_unknown_ids=True)

        assert json.loads(route.calls[0].request.content) == {
            "items": [{"externalId": "temperature:deg_c"}],
            "ignoreUnknownIds": True,
        }
