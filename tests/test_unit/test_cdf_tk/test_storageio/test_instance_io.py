import json
from pathlib import Path

import httpx
import pytest
import responses
import respx
from cognite.client.data_classes.data_modeling import EdgeApply, Node, NodeApply

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceList
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.storageio import InstanceIO, UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import InstanceSpaceSelector, InstanceViewSelector, SelectedView
from cognite_toolkit._cdf_tk.utils.http_client import FailedItem, HTTPClient, SuccessItem


class TestInstanceIO:
    def test_download_instance_ids(self, rsps: responses.RequestsMock, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = toolkit_config.create_api_url("/models/instances/list")
        selector = InstanceViewSelector(
            view=SelectedView(space="mySpace", external_id="myView", version="v42"),
            instance_type="node",
            instance_spaces=("my_insta_space",),
        )
        N = 2500
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
            instances = [
                UploadItem(
                    source_id=f"intance_{i}",
                    item=NodeApply(
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
                    ),
                )
                for i in range(instance_count)
            ]

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

                results = io.upload_items(instances, http_client)

            assert len(results) == instance_count
            failed_items = [res for res in results if isinstance(res, FailedItem)]
            assert len(failed_items) == instance_count // 2
            success_items = [res for res in results if isinstance(res, SuccessItem)]
            assert len(success_items) == instance_count // 2

    @pytest.mark.usefixtures("disable_gzip", "disable_pypi_check")
    def test_download_upload_command(
        self,
        tmp_path: Path,
        toolkit_config: ToolkitClientConfig,
        respx_mock: respx.MockRouter,
        rsps: responses.RequestsMock,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        config = toolkit_config
        client = ToolkitClient(config)
        monkeypatch.setenv("CDF_CLUSTER", config.cdf_cluster)
        monkeypatch.setenv("CDF_PROJECT", config.project)
        some_instance_data = InstanceList(
            [
                Node(
                    space="my_insta_space",
                    external_id=f"node_{i}",
                    version=0,
                    last_updated_time=1,
                    created_time=0,
                    deleted_time=None,
                    properties=None,
                    type=None,
                )
                for i in range(100)
            ]
        )

        def instance_create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            assert items == [asset.as_write().dump() for asset in some_instance_data]
            return httpx.Response(status_code=200, json={"items": some_instance_data.dump()})

        # Download count
        rsps.post(
            config.create_api_url("/models/instances/aggregate"),
            json={
                "items": [
                    {
                        "aggregates": [{"aggregate": "count", "property": "externalId", "value": 100}],
                        "instanceType": "node",
                    }
                ]
            },
            status=200,
        )
        # Download data
        rsps.post(
            config.create_api_url("models/instances/list"),
            json={"items": some_instance_data.dump()},
            status=200,
        )
        # Space
        rsps.post(
            config.create_api_url("/models/spaces/byids"),
            json={"items": [{"space": "my_insta_space", "createdTime": 0, "lastUpdatedTime": 0, "isGlobal": False}]},
            status=200,
        )
        # View
        rsps.post(
            config.create_api_url("/models/views/byids"),
            json={
                "items": [
                    {
                        "space": "my_schema_space",
                        "externalId": "my_view",
                        "version": "v1",
                        "createdTime": 0,
                        "lastUpdatedTime": 0,
                        "description": None,
                        "name": None,
                        "writable": True,
                        "usedFor": "node",
                        "isGlobal": False,
                        "properties": {
                            "name": {
                                "container": {
                                    "space": "my_schema_space",
                                    "externalId": "MyContainer",
                                },
                                "containerPropertyIdentifier": "name",
                                "type": {
                                    "type": "text",
                                    "list": False,
                                },
                                "nullable": True,
                                "immutable": False,
                                "autoIncrement": False,
                            }
                        },
                    }
                ]
            },
        )
        # Container
        rsps.post(
            config.create_api_url("/models/containers/byids"),
            json={
                "items": [
                    {
                        "space": "my_schema_space",
                        "externalId": "MyContainer",
                        "createdTime": 0,
                        "lastUpdatedTime": 0,
                        "description": None,
                        "name": None,
                        "isGlobal": False,
                        "usedFor": "node",
                        "constraints": {},
                        "indexes": {},
                        "properties": {
                            "name": {
                                "type": {
                                    "type": "text",
                                    "list": False,
                                },
                                "nullable": True,
                                "immutable": False,
                                "autoIncrement": False,
                            }
                        },
                    }
                ]
            },
        )

        # Upload data
        respx_mock.post(config.create_api_url(InstanceIO.UPLOAD_ENDPOINT)).mock(side_effect=instance_create_callback)

        selector = InstanceSpaceSelector(
            instance_space="my_insta_space",
            instance_type="node",
            view=SelectedView(space="my_schema_space", external_id="my_view"),
        )
        download_command = DownloadCommand(silent=True, skip_tracking=True)
        upload_command = UploadCommand(silent=True, skip_tracking=True)

        download_command.download(
            selectors=[selector],
            io=InstanceIO(client),
            output_dir=tmp_path,
            verbose=False,
            file_format=".ndjson",
            compression="none",
            limit=100,
        )

        upload_command.upload(
            input_dir=tmp_path / selector.group,
            client=client,
            deploy_resources=False,
            dry_run=False,
            verbose=False,
            kind=InstanceIO.KIND,
        )

        assert len(respx_mock.calls) == 1
