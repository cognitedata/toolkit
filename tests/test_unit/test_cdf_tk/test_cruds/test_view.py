from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import data_modeling as dm

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

    @pytest.mark.parametrize(
        "view_properties,expected_readonly",
        [
            pytest.param(
                {
                    "name": ("name", "CogniteAsset"),
                    "path": ("assetHierarchy_path", "CogniteAsset"),
                    "root": ("assetHierarchy_root", "CogniteAsset"),
                },
                {"path", "root"},
                id="CogniteAsset readonly properties",
            ),
            pytest.param(
                {
                    "name": ("name", "CogniteFile"),
                    "isUploaded": ("isUploaded", "CogniteFile"),
                    "mimeType": ("mimeType", "CogniteFile"),
                },
                {"isUploaded"},
                id="CogniteFile readonly properties",
            ),
            pytest.param(
                {"customProp": ("prop", "CustomContainer")},
                set(),
                id="Custom container no readonly",
            ),
            pytest.param(
                {
                    "assetName": ("name", "CogniteAsset"),
                    "assetPath": ("assetHierarchy_path", "CogniteAsset"),
                    "customProp": ("custom", "CustomContainer"),
                },
                {"assetPath"},
                id="View with custom property identifiers in View filters readonly",
            ),
        ],
    )
    def test_get_readonly_properties(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        view_properties: dict[str, tuple[str, str]],
        expected_readonly: set[str],
    ) -> None:
        """Test that get_readonly_properties identifies readonly properties from containers."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)
        view_id = dm.ViewId(space="test_space", external_id="TestView", version="v1")

        properties = {
            view_prop: dm.MappedProperty(
                container=dm.ContainerId(space="cdf_cdm", external_id=container),
                container_property_identifier=container_prop,
                type=dm.Text(),
                nullable=True,
                immutable=False,
                auto_increment=False,
            )
            for view_prop, (container_prop, container) in view_properties.items()
        }

        view = dm.View(
            space="test_space",
            external_id="TestView",
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
            properties=properties,
        )
        toolkit_client_approval.append(dm.View, [view])

        readonly_props = loader.get_readonly_properties(view_id)
        assert set(readonly_props.keys()) == expected_readonly
