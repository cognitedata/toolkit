from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import data_modeling as dm

from cognite_toolkit._cdf_tk._parameters import read_parameters_from_dict
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, ResourceCRUD, ResourceWorker, SpaceCRUD, ViewCRUD
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestViewLoader:
    @pytest.mark.parametrize(
        "item",
        [
            pytest.param(
                {
                    "filter": {
                        "hasData": [
                            {"type": "container", "space": "sp_my_space", "externalId": "container_id"},
                            {"type": "view", "space": "sp_my_space", "externalId": "view_id"},
                        ]
                    }
                },
                id="HasData Filter",
            ),
            pytest.param(
                {
                    "properties": {
                        "reverseDirectRelation": {
                            "connectionType": "multi_reverse_direct_relation",
                            "source": {
                                "type": "view",
                                "space": "sp_my_space",
                                "externalId": "view_id",
                                "version": "v42",
                            },
                            "through": {
                                "source": {
                                    "type": "view",
                                    "space": "sp_my_space",
                                    "externalId": "view_id",
                                    "version": "v42",
                                },
                                "identifier": "view_property",
                            },
                        }
                    }
                },
                id="Reverse Direct Relation Property",
            ),
        ],
    )
    def test_valid_spec(self, item: dict):
        spec = ViewCRUD.get_write_cls_parameter_spec()
        dumped = read_parameters_from_dict(item)

        extra = dumped - spec

        assert not extra, f"Extra keys: {extra}"

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
