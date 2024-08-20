from collections.abc import Hashable
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import data_modeling as dm

from cognite_toolkit._cdf_tk._parameters import read_parameters_from_dict
from cognite_toolkit._cdf_tk.commands import DeployCommand
from cognite_toolkit._cdf_tk.loaders import ContainerLoader, ResourceLoader, SpaceLoader, ViewLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.test_unit.approval_client import ApprovalCogniteClient


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
        spec = ViewLoader.get_write_cls_parameter_spec()
        dumped = read_parameters_from_dict(item)

        extra = dumped - spec

        assert not extra, f"Extra keys: {extra}"

    def test_update_view_with_interface(self, cognite_client_approval: ApprovalCogniteClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = cognite_client_approval.mock_client
        cdf_tool.client = cognite_client_approval.mock_client
        cdf_tool.toolkit_client = cognite_client_approval.mock_client
        prop1 = dm.MappedProperty(
            dm.ContainerId(space="sp_space", external_id="container_id"),
            "prop1",
            type=dm.Text(),
            nullable=True,
            auto_increment=False,
            immutable=False,
        )
        interface = dm.View(
            space="sp_space",
            external_id="interface",
            version="1",
            properties={"prop1": prop1},
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
        )
        # Note that child views always contain all properties of their parent interfaces.
        child_cdf = dm.View(
            space="sp_space",
            external_id="child",
            version="1",
            properties={"prop1": prop1},
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            filter=None,
            implements=[interface.as_id()],
            writable=True,
            used_for="node",
            is_global=False,
        )
        child_local = dm.ViewApply(
            space="sp_space",
            external_id="child",
            version="1",
            implements=[interface.as_id()],
        )
        # Simulating that the interface and child_cdf are available in CDF
        cognite_client_approval.append(dm.View, [interface, child_cdf])

        loader = ViewLoader.create_loader(cdf_tool, None)
        cmd = DeployCommand(print_warning=False)
        to_create, to_change, unchanged = cmd.to_create_changed_unchanged_triple(
            dm.ViewApplyList([child_local]), loader
        )

        assert len(to_create) == 0
        assert len(to_change) == 0
        assert len(unchanged) == 1

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
                    (SpaceLoader, "sp_my_space"),
                    (ContainerLoader, dm.ContainerId(space="my_container_space", external_id="my_container")),
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
                    (SpaceLoader, "sp_my_space"),
                    (ViewLoader, dm.ViewId(space="my_view_space", external_id="my_view", version="1")),
                    (ViewLoader, dm.ViewId(space="my_other_view_space", external_id="my_edge_view", version="42")),
                ],
                id="View with one container property",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceLoader], Hashable]]) -> None:
        actual = ViewLoader.get_dependent_items(item)

        assert list(actual) == expected

    def test_are_equal_version_int(self, cdf_tool_config: CDFToolConfig) -> None:
        local_view = dm.ViewApply.load("""space: sp_space
externalId: my_view
version: 1""")
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

        loader = ViewLoader.create_loader(cdf_tool_config, None)

        _, local_dumped, cdf_dumped = loader.are_equal(local_view, cdf_view, return_dumped=True)

        assert local_dumped == cdf_dumped
