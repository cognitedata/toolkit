from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import data_modeling as dm

from cognite_toolkit._cdf_tk import constants
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, ResourceCRUD, ResourceWorker, SpaceCRUD, ViewCRUD
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestViewLoader:
    def test_unchanged_view_int_version(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)
        raw_file = """- space: sp_space
  externalId: my_view
  version: 1"""
        file = MagicMock(spec=Path)
        file.read_text.return_value = raw_file
        cdf_view = dm.View(
            space="sp_space",
            external_id="my_view",
            version="1",
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
            properties={},
        )

        toolkit_client_approval.append(dm.View, [cdf_view])

        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([file])
        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}

    @pytest.mark.parametrize(
        "item, expected",
        [
            pytest.param(
                {
                    "space": "sp_my_space",
                    "properties": {
                        "name": {
                            "container": {
                                "type": "container",
                                "space": "my_container_space",
                                "externalId": "my_container",
                            }
                        }
                    },
                },
                [
                    (SpaceCRUD, "sp_my_space"),
                    (ContainerCRUD, dm.ContainerId(space="my_container_space", external_id="my_container")),
                ],
                id="View with one container property",
            ),
            pytest.param(
                {
                    "space": "sp_my_space",
                    "properties": {
                        "toEdge": {
                            "source": {
                                "type": "view",
                                "space": "my_view_space",
                                "externalId": "my_view",
                                "version": "1",
                            },
                            "edgeSource": {
                                "type": "view",
                                "space": "my_other_view_space",
                                "externalId": "my_edge_view",
                                "version": "42",
                            },
                        }
                    },
                },
                [
                    (SpaceCRUD, "sp_my_space"),
                    (ViewCRUD, dm.ViewId(space="my_view_space", external_id="my_view", version="1")),
                    (ViewCRUD, dm.ViewId(space="my_other_view_space", external_id="my_edge_view", version="42")),
                ],
                id="View with one container property",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceCRUD], Hashable]]) -> None:
        actual = ViewCRUD.get_dependent_items(item)

        assert list(actual) == expected

    def test_get_readonly_properties_cognite_asset(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        """Test that get_readonly_properties returns read-only properties for CogniteAsset view."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # Create a view that maps to CogniteAsset container
        view_id = dm.ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1")
        view = dm.View(
            space="cdf_cdm",
            external_id="CogniteAsset",
            version="v1",
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId(space="cdf_cdm", external_id="CogniteAsset"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
                "pathLastUpdatedTime": dm.MappedProperty(
                    container=dm.ContainerId(space="cdf_cdm", external_id="CogniteAsset"),
                    container_property_identifier="assetHierarchy_path_last_updated_time",
                    type=dm.Timestamp(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
                "path": dm.MappedProperty(
                    container=dm.ContainerId(space="cdf_cdm", external_id="CogniteAsset"),
                    container_property_identifier="assetHierarchy_path",
                    type=dm.DirectRelation(is_list=True),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
                "root": dm.MappedProperty(
                    container=dm.ContainerId(space="cdf_cdm", external_id="CogniteAsset"),
                    container_property_identifier="assetHierarchy_root",
                    type=dm.DirectRelation(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
            },
        )

        toolkit_client_approval.append(dm.View, [view])

        readonly_props = loader.get_readonly_properties(view_id)

        # Should return the read-only properties
        assert len(readonly_props) == len(
            constants.READONLY_CONTAINER_PROPERTIES[dm.ContainerId(space="cdf_cdm", external_id="CogniteAsset")]
        )
        assert "pathLastUpdatedTime" in readonly_props
        assert "path" in readonly_props
        assert "root" in readonly_props
        assert "name" not in readonly_props

    def test_get_readonly_properties_cognite_file(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        """Test that get_readonly_properties returns read-only properties for CogniteFile view."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # Create a view that maps to CogniteFile container
        view_id = dm.ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1")
        view = dm.View(
            space="cdf_cdm",
            external_id="CogniteFile",
            version="v1",
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId(space="cdf_cdm", external_id="CogniteFile"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
                "isUploaded": dm.MappedProperty(
                    container=dm.ContainerId(space="cdf_cdm", external_id="CogniteFile"),
                    container_property_identifier="isUploaded",
                    type=dm.Boolean(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
                "uploadedTime": dm.MappedProperty(
                    container=dm.ContainerId(space="cdf_cdm", external_id="CogniteFile"),
                    container_property_identifier="uploadedTime",
                    type=dm.Timestamp(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
            },
        )

        toolkit_client_approval.append(dm.View, [view])

        readonly_props = loader.get_readonly_properties(view_id)

        # Should return the read-only properties
        assert len(readonly_props) == len(
            constants.READONLY_CONTAINER_PROPERTIES[dm.ContainerId(space="cdf_cdm", external_id="CogniteFile")]
        )
        assert "isUploaded" in readonly_props
        assert "uploadedTime" in readonly_props
        assert "name" not in readonly_props

    def test_get_readonly_properties_no_readonly_props(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        """Test that get_readonly_properties returns empty dict for views without read-only properties."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # Create a view without any read-only properties
        view_id = dm.ViewId(space="my_space", external_id="MyView", version="v1")
        view = dm.View(
            space="my_space",
            external_id="MyView",
            version="v1",
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
            properties={
                "name": dm.MappedProperty(
                    container=dm.ContainerId(space="my_space", external_id="MyContainer"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
            },
        )

        toolkit_client_approval.append(dm.View, [view])

        readonly_props = loader.get_readonly_properties(view_id)

        assert len(readonly_props) == 0

    def test_get_readonly_properties_nonexistent_view(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        """Test that get_readonly_properties returns empty dict for nonexistent views."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        view_id = dm.ViewId(space="nonexistent", external_id="NonExistent", version="v1")

        readonly_props = loader.get_readonly_properties(view_id)

        # Should return empty dict without raising an error
        assert len(readonly_props) == 0

    def test_get_readonly_properties_mixed_properties(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        """Test that get_readonly_properties correctly identifies mixed properties."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # Create a view with a mix of properties from different containers and renamed identifier in view
        view_id = dm.ViewId(space="my_space", external_id="MixedView", version="v1")
        view = dm.View(
            space="my_space",
            external_id="MixedView",
            version="v1",
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
            properties={
                # From CogniteAsset container (read-only)
                "assetPath": dm.MappedProperty(
                    container=dm.ContainerId(space="cdf_cdm", external_id="CogniteAsset"),
                    container_property_identifier="assetHierarchy_path",
                    type=dm.Text(is_list=True),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
                # From CogniteAsset container (writable)
                "assetName": dm.MappedProperty(
                    container=dm.ContainerId(space="cdf_cdm", external_id="CogniteAsset"),
                    container_property_identifier="name",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
                # From custom container
                "customProp": dm.MappedProperty(
                    container=dm.ContainerId(space="my_space", external_id="CustomContainer"),
                    container_property_identifier="custom",
                    type=dm.Text(),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                ),
            },
        )

        toolkit_client_approval.append(dm.View, [view])

        readonly_props = loader.get_readonly_properties(view_id)

        # Should only return the read-only property from CogniteAsset
        assert len(readonly_props) == 1
        assert "assetPath" in readonly_props
        assert "assetName" not in readonly_props
        assert "customProp" not in readonly_props
