import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig


@pytest.mark.usefixtures("disable_gzip")
class TestStreamsAPI:
    def test_list_templates(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        template = {
            "name": "ImmutableTestStream",
            "type": "Immutable",
            "maxStreams": {"provisioned": 10},
            "settings": {
                "limits": {"maxRecordsTotal": {"type": "count", "name": "Max records total", "value": 1_000_000}},
                "lifecycle": {"retainedAfterSoftDelete": {"type": "duration", "name": "Retention", "value": "P1D"}},
            },
        }
        respx_mock.get(toolkit_config.create_api_url("/streams/templates")).mock(
            return_value=httpx2.Response(200, json={"items": [template]})
        )

        templates = toolkit_client.streams.list_templates()

        assert [item.dump() for item in templates] == [template]
