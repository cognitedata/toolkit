import json

import httpx2
import pytest
import respx

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.resource_classes.datapoint_subscription import DatapointSubscriptionPartition


@pytest.mark.usefixtures("disable_gzip")
class TestDatapointSubscriptionsAPI:
    def test_list_data(
        self, toolkit_client: ToolkitClient, toolkit_config: ToolkitClientConfig, respx_mock: respx.MockRouter
    ) -> None:
        response = {
            "updates": [
                {
                    "timeSeries": {"id": 1, "externalId": "ts_1", "isString": False, "type": "numeric"},
                    "upserts": [{"timestamp": 10, "value": 1.5}],
                    "deletes": [{"inclusiveBegin": 0, "exclusiveEnd": 5}],
                }
            ],
            "partitions": [{"index": 0, "nextCursor": "next"}],
            "hasNext": False,
        }
        route = respx_mock.post(toolkit_config.create_api_url("/timeseries/subscriptions/data/list")).mock(
            return_value=httpx2.Response(200, json=response)
        )

        result = toolkit_client.datapoint_subscriptions.list_data(
            "my_sub",
            [DatapointSubscriptionPartition(index=0, cursor="prev")],
            limit=10,
            initialize_cursors="2d-ago",
            include_status=True,
        )

        assert {"body": json.loads(route.calls[0].request.content), "result": result.dump()} == {
            "body": {
                "externalId": "my_sub",
                "partitions": [{"index": 0, "cursor": "prev"}],
                "limit": 10,
                "initializeCursors": "2d-ago",
                "includeStatus": True,
            },
            "result": response,
        }

    @pytest.mark.parametrize("limit", [0, 100_001])
    def test_list_data_rejects_invalid_limit(self, toolkit_client: ToolkitClient, limit: int) -> None:
        with pytest.raises(ValueError, match="Limit must be between 1 and 100000"):
            toolkit_client.datapoint_subscriptions.list_data(
                "my_sub", [DatapointSubscriptionPartition(index=0)], limit=limit
            )
