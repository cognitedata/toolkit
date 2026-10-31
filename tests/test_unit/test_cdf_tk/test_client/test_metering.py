import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.api.metering import MeteringAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient
from cognite_toolkit._cdf_tk.client.identifiers import MeterId

_METER = {
    "meterId": "atlas.monthly_ai_tokens",
    "datapoints": [{"timestamp": 1730204346000, "average": 42.5}],
}


@pytest.mark.usefixtures("disable_gzip")
class TestMeteringAPI:
    def test_list_and_retrieve(self, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter) -> None:
        api = MeteringAPI(HTTPClient(toolkit_config))
        respx_mock.post(api._make_url("/metering/meters/list")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_METER]})
        )
        respx_mock.post(api._make_url("/metering/meters/byids")).mock(
            return_value=httpx2.Response(status_code=200, json={"items": [_METER]})
        )

        listed = api.list(prefix="atlas.", start=0, number_of_datapoints=10, limit=5)
        retrieved = api.retrieve([MeterId(meter_id="atlas.monthly_ai_tokens")], start=0, end=100)

        assert [item.dump() for item in listed] == [_METER]
        assert [item.dump() for item in retrieved] == [_METER]
        assert respx_mock.calls[0].request.headers["cdf-version"] == "20230101-alpha"
        assert json.loads(respx_mock.calls[0].request.content) == {
            "filter": {"prefix": {"property": ["meterId"], "value": "atlas."}},
            "start": 0,
            "numberOfDatapoints": 10,
            "limit": 5,
        }
        assert json.loads(respx_mock.calls[1].request.content) == {
            "items": [{"meterId": "atlas.monthly_ai_tokens"}],
            "start": 0,
            "end": 100,
        }

    def test_invalid_number_of_datapoints(self, toolkit_config: ToolkitClientConfig) -> None:
        api = MeteringAPI(HTTPClient(toolkit_config))
        with pytest.raises(ValueError, match="number_of_datapoints"):
            api.list(number_of_datapoints=601)
