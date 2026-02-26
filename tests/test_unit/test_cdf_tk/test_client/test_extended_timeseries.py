from collections.abc import Sequence

import pytest
import respx
from httpx import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import NodeReference
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesResponse
from tests.constants import CDF_PROJECT


class TestTimeSeriesAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_set_pending_ids(
        self,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
    ) -> None:
        client = ToolkitClient(config=toolkit_config)
        url = f"{toolkit_config.base_url}/api/v1/projects/{CDF_PROJECT}/timeseries/set-pending-instance-ids"
        respx_mock.post(url).mock(
            return_value=Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "externalId": "my_ts",
                            "id": 123,
                            "type": "numeric",
                            "createdTime": 0,
                            "lastUpdatedTime": 0,
                            "pendingInstanceId": {"space": "my_space", "externalId": "myExternalId"},
                        }
                    ]
                },
            )
        )

        items = [
            PendingInstanceId(
                pending_instance_id=NodeReference(space="my_space", external_id="myExternalId"),
                external_id="my_ts",
            ),
        ]
        result = client.tool.timeseries.set_pending_ids(items)

        assert len(result) == 1
        assert isinstance(result[0], TimeSeriesResponse)

    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize(
        "items",
        [
            pytest.param([InternalId(id=123)], id="Single ID"),
            pytest.param([InternalId(id=123), InternalId(id=456)], id="Multiple IDs"),
            pytest.param([ExternalId(external_id="my_ts")], id="Single External ID"),
            pytest.param(
                [ExternalId(external_id="my_ts"), ExternalId(external_id="other")],
                id="Multiple External IDs",
            ),
        ],
    )
    def test_unlink_instance_ids(
        self,
        items: Sequence[InternalOrExternalId],
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
    ) -> None:
        client = ToolkitClient(config=toolkit_config)
        url = f"{toolkit_config.base_url}/api/v1/projects/{CDF_PROJECT}/timeseries/unlink-instance-ids"
        respx_mock.post(url).mock(
            return_value=Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "externalId": "does-not-matter",
                            "id": 123,
                            "type": "numeric",
                            "createdTime": 0,
                            "lastUpdatedTime": 0,
                        }
                    ]
                },
            )
        )

        result = client.tool.timeseries.unlink_instance_ids(items)

        assert len(result) == 1
        assert isinstance(result[0], TimeSeriesResponse)
