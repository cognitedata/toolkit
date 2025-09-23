from unittest.mock import MagicMock
from warnings import catch_warnings

import pytest
from cognite.client.data_classes.data_modeling import NodeId, ViewId
from cognite.client.data_classes.data_modeling.instances import Node, Properties

from cognite_toolkit._cdf_tk.client.api.extended_data_modeling import ExtendedInstancesAPI
from cognite_toolkit._cdf_tk.client.api.migration import ResourceViewMappingAPI
from cognite_toolkit._cdf_tk.client.data_classes.migration import ResourceViewMapping, ResourceViewMappingApply
from cognite_toolkit._cdf_tk.constants import COGNITE_MIGRATION_SPACE


@pytest.fixture()
def instance_api() -> MagicMock:
    return MagicMock(spec=ExtendedInstancesAPI)


class TestViewSource:
    def test_upsert(self, instance_api: MagicMock) -> None:
        view_source_api = ResourceViewMappingAPI(instance_api=instance_api)
        view_source = ResourceViewMappingApply(
            external_id="asset_mapping",
            resource_type="asset",
            view_id=ViewId("cdf_cdm", "CogniteAsset", "v1"),
            property_mapping={
                "name": "name",
                "description": "description",
            },
        )
        _ = view_source_api.upsert(view_source)

        assert instance_api.apply.called
        args, kwargs = instance_api.apply.call_args
        assert args[0] == view_source
        assert kwargs == {"skip_on_version_conflict": False, "replace": False}

    def test_retrieve_single_valid(self, instance_api: MagicMock) -> None:
        view_source_api = ResourceViewMappingAPI(instance_api=instance_api)
        mock_view_source = ResourceViewMapping(
            external_id="test_source",
            version=1,
            last_updated_time=1000,
            created_time=1000,
            resource_type="asset",
            view_id=ViewId("cdf_cdm", "CogniteAsset", "v1"),
            property_mapping={"name": "name"},
        )
        instance_api.retrieve_nodes.return_value = mock_view_source

        result = view_source_api.retrieve("test_source")
        assert result == mock_view_source
        instance_api.retrieve_nodes.assert_called_once()
        args, kwargs = instance_api.retrieve_nodes.call_args
        assert args[0] == mock_view_source.as_id()
        assert kwargs == {"node_cls": ResourceViewMapping}

    def test_retrieve_multiple_valid(self, instance_api: MagicMock) -> None:
        view_source_api = ResourceViewMappingAPI(instance_api=instance_api)
        mock_view_sources = [
            Node(
                space=COGNITE_MIGRATION_SPACE,
                external_id=f"test_source_{i}",
                version=1,
                last_updated_time=1000,
                created_time=1000,
                deleted_time=None,
                type=None,
                properties=Properties(
                    {
                        ResourceViewMappingApply.get_source(): {
                            "resourceType": "asset",
                            "viewId": ViewId("cdf_cdm", "CogniteAsset", "v1").dump(),
                            "propertyMapping": {"name": "name"},
                        }
                    }
                ),
            )
            for i in range(2)
        ]
        instance_api.retrieve.return_value.nodes = mock_view_sources

        result = view_source_api.retrieve(["test_source_0", "test_source_1"])
        assert len(result) == 2
        assert all(isinstance(r, ResourceViewMapping) for r in result)
        instance_api.retrieve.assert_called_once()

    def test_retrieve_single_invalid(self, instance_api: MagicMock) -> None:
        view_source_api = ResourceViewMappingAPI(instance_api=instance_api)
        instance_api.retrieve_nodes.side_effect = ValueError(
            "Invalid viewId format. Expected 'space', 'externalId', 'version'. Error: 'some_error'"
        )

        with pytest.raises(ValueError) as exc_info:
            _ = view_source_api.retrieve("invalid_source")

        assert (
            str(exc_info.value)
            == "Invalid viewId format. Expected 'space', 'externalId', 'version'. Error: 'some_error'"
        )

    def test_retrieve_multiple_some_invalid(self, instance_api: MagicMock) -> None:
        default_args = dict(
            space=COGNITE_MIGRATION_SPACE,
            version=1,
            last_updated_time=1000,
            created_time=1000,
            deleted_time=None,
            type=None,
        )
        view_source_api = ResourceViewMappingAPI(instance_api=instance_api)
        mock_view_sources = [
            Node(
                external_id="valid_source",
                **default_args,
                properties=Properties(
                    {
                        ResourceViewMappingApply.get_source(): {
                            "resourceType": "asset",
                            "viewId": ViewId("cdf_cdm", "CogniteAsset", "v1").dump(),
                            "propertyMapping": {"name": "name"},
                        }
                    }
                ),
            ),
            Node(
                external_id="invalid_source",
                **default_args,
                properties=Properties(
                    {
                        ResourceViewMappingApply.get_source(): {
                            "resourceType": "asset",
                            "viewId": "invalid_view_id_format",
                            "propertyMapping": {"Not a valid mapping": {"name": "name"}},
                        }
                    }
                ),
            ),
        ]
        instance_api.retrieve.return_value.nodes = mock_view_sources

        with catch_warnings(record=True) as record:
            result = view_source_api.retrieve(["valid_source", "invalid_source"])
        assert len(result) == 1
        first = result[0]
        assert isinstance(first, ResourceViewMapping)
        assert first.external_id == "valid_source"
        assert len(record) == 1

    def test_delete_single(self, instance_api: MagicMock) -> None:
        view_source_api = ResourceViewMappingAPI(instance_api=instance_api)
        instance_api.delete.return_value.nodes = ["deleted_node_id"]

        result = view_source_api.delete("test_source")
        assert result == "deleted_node_id"
        instance_api.delete.assert_called_once()
        args, _ = instance_api.delete.call_args
        assert args[0] == NodeId(COGNITE_MIGRATION_SPACE, "test_source")

    def test_delete_multiple(self, instance_api: MagicMock) -> None:
        view_source_api = ResourceViewMappingAPI(instance_api=instance_api)
        instance_api.delete.return_value.nodes = ["deleted_node_1", "deleted_node_2"]

        result = view_source_api.delete(["test_source_1", "test_source_2"])
        assert len(result) == 2
        instance_api.delete.assert_called_once()
        args, _ = instance_api.delete.call_args
        assert args[0] == [
            NodeId(COGNITE_MIGRATION_SPACE, "test_source_1"),
            NodeId(COGNITE_MIGRATION_SPACE, "test_source_2"),
        ]
