import json
from pathlib import Path

import httpx
import pytest
import responses
import respx
from cognite.client.data_classes.data_modeling import EdgeApply, NodeApply

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.http_client import (
    HTTPClient,
    ItemsFailedResponse,
    ItemsSuccessResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    NodeResponse,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._data_model import DataModelResponseWithViews
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.storageio import InstanceIO, UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import InstanceSpaceSelector, InstanceViewSelector, SelectedView
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestInstanceIO:
    def test_download_instance_ids(
        self, rsps: responses.RequestsMock, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig
    ) -> None:
        client = ToolkitClient(config=toolkit_config)
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

        respx_mock.post(url).side_effect = [
            httpx.Response(
                status_code=200,
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
                        for i in range(1000)
                    ],
                    "nextCursor": "cursor_1",
                },
            ),
            httpx.Response(
                status_code=200,
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
                        for i in range(1000, 2000)
                    ],
                    "nextCursor": "cursor_2",
                },
            ),
            httpx.Response(
                status_code=200,
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
                        for i in range(2000, N)
                    ]
                },
            ),
        ]
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
        client = ToolkitClient(config=toolkit_config)
        instance_count = 12
        with HTTPClient(config) as http_client:
            instances = (
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
            )
            upload_items = [UploadItem(source_id=instance.external_id, item=instance) for instance in instances]

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
                results = io.upload_items(upload_items, http_client)

            assert len(results) == instance_count
            failed_items = [id for res in results if isinstance(res, ItemsFailedResponse) for id in res.ids]
            assert len(failed_items) == instance_count // 2
            success_items = [id for res in results if isinstance(res, ItemsSuccessResponse) for id in res.ids]
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
        some_instance_data = [
            NodeResponse(
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

        def instance_create_callback(request: httpx.Request) -> httpx.Response:
            payload = json.loads(request.content)
            assert "items" in payload
            items = payload["items"]
            assert isinstance(items, list)
            assert len(items) == len(some_instance_data)
            assert {item["externalId"] for item in items} == {inst.external_id for inst in some_instance_data}
            return httpx.Response(status_code=200, json={"items": [inst.dump() for inst in some_instance_data]})

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
        respx_mock.post(config.create_api_url("/models/instances/list")).respond(
            json={"items": [inst.dump() for inst in some_instance_data]},
            status_code=200,
        )
        # Space
        respx_mock.post(config.create_api_url("/models/spaces/byids")).respond(
            json={"items": [{"space": "my_insta_space", "createdTime": 0, "lastUpdatedTime": 0, "isGlobal": False}]},
            status_code=200,
        )
        # View
        respx_mock.post(
            config.create_api_url("/models/views/byids"),
        ).respond(
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
                        "mappedContainers": [],
                        "queryable": True,
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
                                "constraintState": {},
                            }
                        },
                    }
                ]
            },
        )
        # Container
        respx_mock.post(
            config.create_api_url("/models/containers/byids"),
        ).respond(
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
            view=SelectedView(space="my_schema_space", external_id="my_view", version="v1"),
            download_dir_name="instances",
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
            input_dir=tmp_path / selector.download_dir_name,
            client=client,
            deploy_resources=False,
            dry_run=False,
            verbose=False,
            kind=InstanceIO.KIND,
        )

        assert len(respx_mock.calls) == 4

    def test_stream_data_with_edges(self, respx_mock: respx.MockRouter, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config)
        selector = InstanceViewSelector(
            view=SelectedView(space="mySpace", external_id="myView", version="v42"),
            instance_type="node",
            instance_spaces=("my_space",),
            include_edges=True,
        )

        view_url = toolkit_config.create_api_url("/models/views/byids")
        query_url = toolkit_config.create_api_url("/models/instances/query")

        respx_mock.post(view_url).respond(
            status_code=200,
            json={
                "items": [
                    {
                        "space": "mySpace",
                        "externalId": "myView",
                        "version": "v42",
                        "createdTime": 0,
                        "lastUpdatedTime": 0,
                        "writable": True,
                        "usedFor": "node",
                        "isGlobal": False,
                        "queryable": True,
                        "mappedContainers": [],
                        "properties": {
                            "name": {
                                "container": {"space": "mySpace", "externalId": "MyContainer"},
                                "containerPropertyIdentifier": "name",
                                "type": {"type": "text", "list": False},
                                "nullable": True,
                                "immutable": False,
                                "autoIncrement": False,
                                "constraintState": {},
                            },
                            "children": {
                                "connectionType": "multi_edge_connection",
                                "source": {"space": "mySpace", "externalId": "myView", "version": "v42"},
                                "type": {"space": "mySpace", "externalId": "edge_type"},
                                "direction": "outwards",
                            },
                        },
                    }
                ]
            },
        )

        def _node(ext_id: str) -> dict:
            return {
                "instanceType": "node",
                "space": "my_space",
                "externalId": ext_id,
                "version": 1,
                "createdTime": 0,
                "lastUpdatedTime": 0,
            }

        def _edge(ext_id: str, start: str, end: str) -> dict:
            return {
                "instanceType": "edge",
                "space": "my_space",
                "externalId": ext_id,
                "version": 1,
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "type": {"space": "mySpace", "externalId": "edge_type"},
                "startNode": {"space": "my_space", "externalId": start},
                "endNode": {"space": "my_space", "externalId": end},
            }

        respx_mock.post(query_url).side_effect = [
            # Call 1: first node batch + first edge batch (edge cursor present)
            httpx.Response(
                status_code=200,
                json={
                    "items": {
                        "nodes": [_node("node_0"), _node("node_1")],
                        "children": [_edge("edge_0", "node_0", "node_1"), _edge("edge_1", "node_1", "node_0")],
                    },
                    "nextCursor": {"nodes": "node_cursor_1", "children": "edge_cursor_1"},
                },
            ),
            # Call 2: second edge batch (no more edge cursor → exhausts edges for first node batch)
            httpx.Response(
                status_code=200,
                json={
                    "items": {
                        "nodes": [],
                        "children": [_edge("edge_2", "node_0", "node_1")],
                    },
                    "nextCursor": {"nodes": "node_cursor_1"},
                },
            ),
            # Call 3: second node batch (no more cursors → done)
            httpx.Response(
                status_code=200,
                json={
                    "items": {
                        "nodes": [_node("node_2"), _node("node_3")],
                        "children": [],
                    },
                    "nextCursor": {},
                },
            ),
        ]

        io = InstanceIO(client)
        pages = list(io.stream_data(selector))

        assert len(pages) == 2

        first_page = pages[0]
        first_node_ids = {item.external_id for item in first_page.items if item.instance_type == "node"}
        first_edge_ids = {item.external_id for item in first_page.items if item.instance_type == "edge"}
        assert first_node_ids == {"node_0", "node_1"}
        assert first_edge_ids == {"edge_0", "edge_1", "edge_2"}
        assert first_page.next_cursor == "node_cursor_1"

        second_page = pages[1]
        second_node_ids = {item.external_id for item in second_page.items if item.instance_type == "node"}
        second_edge_ids = {item.external_id for item in second_page.items if item.instance_type == "edge"}
        assert second_node_ids == {"node_2", "node_3"}
        assert second_edge_ids == set()

    @pytest.mark.parametrize(
        "item_json,expected_properties",
        [
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "asset_node",
                    "instanceType": "node",
                    "sources": [
                        {
                            "source": {"type": "container", "space": "cdf_cdm", "externalId": "CogniteAsset"},
                            "properties": {
                                "name": "Asset",
                                "assetHierarchy_path": ["root", "child"],
                                "assetHierarchy_root": "root",
                                "description": "Test",
                            },
                        }
                    ],
                },
                {"name": "Asset", "description": "Test"},
                id="CogniteAsset container filters readonly props",
            ),
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "file_node",
                    "instanceType": "node",
                    "sources": [
                        {
                            "source": {"type": "container", "space": "cdf_cdm", "externalId": "CogniteFile"},
                            "properties": {"name": "File", "isUploaded": True, "uploadedTime": 123, "mimeType": "pdf"},
                        }
                    ],
                },
                {"name": "File", "mimeType": "pdf"},
                id="CogniteFile container filters readonly props",
            ),
            pytest.param(
                {
                    "space": "my_space",
                    "externalId": "asset_node",
                    "instanceType": "node",
                    "sources": [
                        {
                            "source": {
                                "type": "view",
                                "space": "cdf_cdm",
                                "externalId": "CogniteAsset",
                                "version": "v1",
                            },
                            "properties": {
                                "name": "Asset",
                                "path": ["root", "child"],
                                "pathLastUpdatedTime": 123456789,
                            },
                        }
                    ],
                },
                {"name": "Asset"},
                id="CogniteAsset view filters readonly props",
            ),
        ],
    )
    def test_json_to_resource_filters_readonly_properties(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        item_json: dict,
        expected_properties: dict,
        cognite_core_no_3D: DataModelResponseWithViews,
    ) -> None:
        """Test that json_to_resource filters out read-only properties from containers."""
        toolkit_client_approval.append(ViewResponse, cognite_core_no_3D.views)
        instance_io = InstanceIO(toolkit_client_approval.mock_client)
        instance = instance_io.json_to_resource(item_json)

        assert dict(instance.sources[0].properties) == expected_properties
