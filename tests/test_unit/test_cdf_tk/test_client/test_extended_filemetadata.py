from collections.abc import Sequence

import pytest
import respx
from httpx import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.instance_api import NodeReference
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from tests.constants import CDF_PROJECT


class TestFileMetadataAPI:
    @pytest.mark.usefixtures("disable_gzip")
    def test_set_pending_ids(
        self,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
    ) -> None:
        client = ToolkitClient(config=toolkit_config)
        url = f"{toolkit_config.base_url}/api/v1/projects/{CDF_PROJECT}/files/set-pending-instance-ids"
        respx_mock.post(url).mock(
            return_value=Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "externalId": "my_file",
                            "name": "my_file",
                            "id": 123,
                            "createdTime": 0,
                            "lastUpdatedTime": 0,
                            "uploaded": True,
                            "pendingInstanceId": {"space": "my_space", "externalId": "myExternalId"},
                        }
                    ]
                },
            )
        )

        items = [
            PendingInstanceId(
                pending_instance_id=NodeReference(space="my_space", external_id="myExternalId"),
                external_id="my_file",
            ),
        ]
        result = client.tool.filemetadata.set_pending_ids(items)

        assert len(result) == 1
        assert isinstance(result[0], FileMetadataResponse)

    @pytest.mark.usefixtures("disable_gzip")
    @pytest.mark.parametrize(
        "items",
        [
            pytest.param([InternalId(id=123)], id="Single ID"),
            pytest.param([InternalId(id=123), InternalId(id=456)], id="Multiple IDs"),
            pytest.param([ExternalId(external_id="my_file")], id="Single External ID"),
            pytest.param(
                [ExternalId(external_id="my_file"), ExternalId(external_id="other")],
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
        url = f"{toolkit_config.base_url}/api/v1/projects/{CDF_PROJECT}/files/unlink-instance-ids"
        respx_mock.post(url).mock(
            return_value=Response(
                status_code=200,
                json={
                    "items": [
                        {
                            "externalId": "does-not-matter",
                            "name": "my_file",
                            "id": 123,
                            "createdTime": 0,
                            "lastUpdatedTime": 0,
                            "uploaded": True,
                        }
                    ]
                },
            )
        )

        result = client.tool.filemetadata.unlink_instance_ids(items)

        assert len(result) == 1
        assert isinstance(result[0], FileMetadataResponse)
