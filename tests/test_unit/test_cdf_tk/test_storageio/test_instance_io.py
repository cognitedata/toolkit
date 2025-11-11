import json
from pathlib import Path
from unittest.mock import MagicMock

import httpx
import pytest
import responses
import respx
from cognite.client.data_classes.data_modeling import EdgeApply, MappedProperty, Node, NodeApply, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.instances import InstanceApplyList, InstanceList
from cognite_toolkit._cdf_tk.commands import DownloadCommand, UploadCommand
from cognite_toolkit._cdf_tk.cruds import ViewCRUD
from cognite_toolkit._cdf_tk.storageio import InstanceIO
from cognite_toolkit._cdf_tk.storageio.selectors import InstanceSpaceSelector, InstanceViewSelector, SelectedView
from cognite_toolkit._cdf_tk.utils.http_client import FailedResponseItems, HTTPClient, SuccessResponseItems


@pytest.fixture
def instance_io(toolkit_config: ToolkitClientConfig) -> InstanceIO:
    """Fixture that provides an InstanceIO instance for testing."""
    client = ToolkitClient(config=toolkit_config)
    return InstanceIO(client)


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
                results = io.upload_items(instances, http_client)

            assert len(results) == instance_count
            failed_items = [id for res in results if isinstance(res, FailedResponseItems) for id in res.ids]
            assert len(failed_items) == instance_count // 2
            success_items = [id for res in results if isinstance(res, SuccessResponseItems) for id in res.ids]
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

    def test_filter_readonly_properties_from_view(
        self, instance_io: InstanceIO, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that read-only properties are filtered from view sources."""

        # Mock the ViewCRUD.create_loader to return a mock with readonly properties
        mock_view_crud = MagicMock()
        mock_view_crud.get_readonly_properties.return_value = {
            "pathLastUpdatedTime": MagicMock(spec=MappedProperty),
            "path": MagicMock(spec=MappedProperty),
        }

        monkeypatch.setattr(ViewCRUD, "create_loader", MagicMock(return_value=mock_view_crud))

        item_json = {
            "space": "my_space",
            "externalId": "my_node",
            "instanceType": "node",
            "existingVersion": 1,
            "sources": [
                {
                    "source": {
                        "type": "view",
                        "space": "cdf_cdm",
                        "externalId": "CogniteAsset",
                        "version": "v1",
                    },
                    "properties": {
                        "name": "My Asset",
                        "pathLastUpdatedTime": 123456789,
                        "path": ["asset1", "asset2"],
                        "description": "Test asset",
                    },
                }
            ],
        }

        instance_io._filter_readonly_properties(item_json)

        # Verify read-only properties were removed
        assert "name" in item_json["sources"][0]["properties"]
        assert "description" in item_json["sources"][0]["properties"]
        assert "pathLastUpdatedTime" not in item_json["sources"][0]["properties"]
        assert "path" not in item_json["sources"][0]["properties"]

    def test_filter_readonly_properties_from_container(self, instance_io: InstanceIO) -> None:
        """Test that read-only properties are filtered from container sources."""

        item_json = {
            "space": "my_space",
            "externalId": "my_node",
            "instanceType": "node",
            "existingVersion": 1,
            "sources": [
                {
                    "source": {
                        "type": "container",
                        "space": "cdf_cdm",
                        "externalId": "CogniteAsset",
                    },
                    "properties": {
                        "name": "My Asset",
                        "assetHierarchy_path_last_updated_time": 123456789,
                        "assetHierarchy_path": ["asset1", "asset2"],
                        "assetHierarchy_root": "asset1",
                        "description": "Test asset",
                    },
                }
            ],
        }

        instance_io._filter_readonly_properties(item_json)

        # Verify read-only properties were removed
        assert "name" in item_json["sources"][0]["properties"]
        assert "description" in item_json["sources"][0]["properties"]
        assert "assetHierarchy_path_last_updated_time" not in item_json["sources"][0]["properties"]
        assert "assetHierarchy_path" not in item_json["sources"][0]["properties"]
        assert "assetHierarchy_root" not in item_json["sources"][0]["properties"]

    def test_filter_readonly_properties_cognite_file(self, instance_io: InstanceIO) -> None:
        """Test that read-only properties are filtered from CogniteFile container."""

        item_json = {
            "space": "my_space",
            "externalId": "my_file",
            "instanceType": "node",
            "existingVersion": 1,
            "sources": [
                {
                    "source": {
                        "type": "container",
                        "space": "cdf_cdm",
                        "externalId": "CogniteFile",
                    },
                    "properties": {
                        "name": "My File",
                        "isUploaded": True,
                        "uploadedTime": 123456789,
                        "mimeType": "application/pdf",
                    },
                }
            ],
        }

        instance_io._filter_readonly_properties(item_json)

        # Verify read-only properties were removed
        assert "name" in item_json["sources"][0]["properties"]
        assert "mimeType" in item_json["sources"][0]["properties"]
        assert "isUploaded" not in item_json["sources"][0]["properties"]
        assert "uploadedTime" not in item_json["sources"][0]["properties"]

    def test_filter_readonly_properties_multiple_sources(
        self, instance_io: InstanceIO, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Test that read-only properties are filtered from multiple sources."""

        # Mock the ViewCRUD.create_loader to return a mock with readonly properties
        mock_view_crud = MagicMock()
        mock_view_crud.get_readonly_properties.return_value = {
            "pathLastUpdatedTime": MagicMock(spec=MappedProperty),
            "path": MagicMock(spec=MappedProperty),
        }

        monkeypatch.setattr(ViewCRUD, "create_loader", MagicMock(return_value=mock_view_crud))

        item_json = {
            "space": "my_space",
            "externalId": "my_node",
            "instanceType": "node",
            "existingVersion": 2,
            "sources": [
                {
                    "source": {
                        "type": "view",
                        "space": "cdf_cdm",
                        "externalId": "CogniteAsset",
                        "version": "v1",
                    },
                    "properties": {
                        "name": "My Asset",
                        "path": ["asset1", "asset2"],
                    },
                },
                {
                    "source": {
                        "type": "container",
                        "space": "my_space",
                        "externalId": "CustomContainer",
                    },
                    "properties": {
                        "customProp": "value",
                    },
                },
            ],
        }

        instance_io._filter_readonly_properties(item_json)

        # Verify read-only properties were removed only from the CogniteAsset source
        assert "name" in item_json["sources"][0]["properties"]
        assert "assetHierarchy_path" not in item_json["sources"][0]["properties"]
        assert "customProp" in item_json["sources"][1]["properties"]

    def test_filter_readonly_properties_no_sources(self, instance_io: InstanceIO) -> None:
        """Test that filtering handles instances without sources gracefully."""

        item_json = {
            "space": "my_space",
            "externalId": "my_node",
            "instanceType": "node",
            "existingVersion": 7,
        }

        # Should not raise an error
        instance_io._filter_readonly_properties(item_json)

    def test_filter_readonly_properties_empty_sources(self, instance_io: InstanceIO) -> None:
        """Test that filtering handles instances with an empty sources list gracefully."""

        item_json = {
            "space": "my_space",
            "externalId": "my_node",
            "instanceType": "node",
            "existingVersion": 7,
            "sources": [],
        }

        # Should not raise an error
        instance_io._filter_readonly_properties(item_json)
        assert item_json["sources"] == []

    def test_filter_readonly_properties_caching(self, instance_io: InstanceIO, monkeypatch: pytest.MonkeyPatch) -> None:
        """Test that view readonly properties are cached to avoid redundant lookups."""

        # Mock the ViewCRUD.create_loader to return a mock with readonly properties
        mock_view_crud = MagicMock()
        mock_view_crud.get_readonly_properties.return_value = {
            "readOnlyProp": MagicMock(spec=MappedProperty),
        }

        monkeypatch.setattr(ViewCRUD, "create_loader", MagicMock(return_value=mock_view_crud))

        view_id = ViewId(space="my_space", external_id="MyView", version="v1")

        item_json_1 = {
            "space": "my_space",
            "externalId": "node_1",
            "instanceType": "node",
            "existingVersion": 0,
            "sources": [
                {
                    "source": view_id.dump(),
                    "properties": {"readOnlyProp": "value1", "normalProp": "value2"},
                }
            ],
        }

        item_json_2 = {
            "space": "my_space",
            "externalId": "node_2",
            "instanceType": "node",
            "existingVersion": 7,
            "sources": [
                {
                    "source": view_id.dump(),
                    "properties": {"readOnlyProp": "value3", "normalProp": "value4"},
                }
            ],
        }

        # First call should fetch from ViewCRUD
        instance_io._filter_readonly_properties(item_json_1)
        assert mock_view_crud.get_readonly_properties.call_count == 1

        # Second call should use cache
        instance_io._filter_readonly_properties(item_json_2)
        assert mock_view_crud.get_readonly_properties.call_count == 1  # Still 1, not 2

        # Verify both items were filtered correctly
        assert "readOnlyProp" not in item_json_1["sources"][0]["properties"]
        assert "normalProp" in item_json_1["sources"][0]["properties"]
        assert "readOnlyProp" not in item_json_2["sources"][0]["properties"]
        assert "normalProp" in item_json_2["sources"][0]["properties"]
