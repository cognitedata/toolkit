import json

import httpx
import pytest
import responses
import respx
from cognite.client.data_classes.data_modeling import EdgeApply, NodeApply, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList
from cognite_toolkit._cdf_tk.storageio import InstanceIO, InstanceViewSelector
from cognite_toolkit._cdf_tk.utils.http_client import FailedItem, HTTPClient, SuccessItem


class TestInstanceIO:
    def test_download_instance_ids(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = toolkit_config.create_api_url("/models/instances/list")
        selector = InstanceViewSelector(
            ViewId("mySpace", "myView", "v42"), instance_type="node", instance_spaces=("my_insta_space",)
        )
        N = 2500
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                toolkit_config.create_api_url("/models/instances/aggregate"),
                status=200,
                json={
                    "items": [
                        {
                            "instanceType": "node",
                            "aggregates": [{"aggregate": "count", "property": "externalId", "value": N}],
                        }
                    ]
                },
            )
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={
                    "items": [
                        {
                            "externalId": f"instance_{i}",
                            "space": "my_space",
                            "instanceType": "node",
                            "createdTime": 0,
                            "lastUpdatedTime": 0,
                            "version": 1,
                        }
                        for i in range(N)
                    ]
                },
            )
            io = InstanceIO(client)
            ids = list(io.download_ids(selector))
            count = io.count(selector)
        assert len(list(ids)) == N // io.CHUNK_SIZE + (1 if N % io.CHUNK_SIZE > 0 else 0)
        total_ids = sum(len(chunk) for chunk in ids)
        assert total_ids == N
        assert count == N

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_upload_force(self, toolkit_config: ToolkitClientConfig) -> None:
        config = toolkit_config
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        instance_count = 12
        with HTTPClient(config) as http_client:
            instances = InstanceApplyList(
                [
                    NodeApply(
                        space="my_space",
                        external_id=f"instance_{i}",
                    )
                    if i % 2 == 0
                    else EdgeApply(
                        space="my_space",
                        external_id=f"instance_{i}",
                        type=("schema_space", "edge_type"),
                        start_node=("my_space", f"instance_{i - 1}"),
                        end_node=("my_space", f"instance_{i + 1}"),
                    )
                    for i in range(instance_count)
                ]
            )

            def hate_edges(request: httpx.Request) -> httpx.Response:
                # Check request body content
                body_content = request.content.decode() if request.content else ""
                if "edge" in body_content:
                    return httpx.Response(
                        400, json={"error": {"code": "InvalidArgument", "message": "I do not like edges!"}}
                    )
                else:
                    items = json.loads(body_content).get("items", [])
                    response_data = {
                        "items": [
                            {
                                "instanceType": "node",
                                "space": item["space"],
                                "externalId": item["externalId"],
                                "wasModified": True,
                                "createdTime": 0,
                                "lastUpdatedTime": 0,
                            }
                            for item in items
                        ]
                    }
                    return httpx.Response(200, json=response_data)

            url = toolkit_config.create_api_url("/models/instances")

            with respx.mock() as rsps:
                rsps.post(url).mock(side_effect=hate_edges)
                io = InstanceIO(client)
                results = io.upload_items_force(instances, http_client)

            assert len(results) == instance_count
            failed_items = [res for res in results if isinstance(res, FailedItem)]
            assert len(failed_items) == instance_count // 2
            success_items = [res for res in results if isinstance(res, SuccessItem)]
            assert len(success_items) == instance_count // 2
