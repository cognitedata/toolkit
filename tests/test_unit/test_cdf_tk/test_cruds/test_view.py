from collections.abc import Hashable
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ConstraintOrIndexState,
    ContainerPropertyDefinition,
    ContainerReference,
    ContainerResponse,
    DirectNodeRelation,
    RequiresConstraintDefinition,
    SpaceReference,
    TextProperty,
    ViewCorePropertyResponse,
    ViewReference,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._data_model import DataModelResponseWithViews
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, ResourceCRUD, ResourceWorker, SpaceCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitCycleError
from tests.test_unit.approval_client import ApprovalToolkitClient


def _create_test_container(
    external_id: str,
    properties: dict[str, ContainerPropertyDefinition],
    constraints: dict | None = None,
    space: str = "my_space",
) -> ContainerResponse:
    """Helper to create a Container with standard test defaults."""
    return ContainerResponse(
        space=space,
        external_id=external_id,
        properties=properties,
        last_updated_time=1,
        created_time=1,
        is_global=False,
        used_for="node",
        constraints=constraints or {},
        description=None,
        name=None,
        indexes={},
    )


def _create_test_view(external_id: str, container: ContainerResponse) -> ViewResponse:
    """Helper to create a View that maps to all properties of the given Container."""
    properties = {}
    for prop_name, container_prop in container.properties.items():
        properties[prop_name] = ViewCorePropertyResponse(
            container=container.as_id(),
            container_property_identifier=prop_name,
            type=container_prop.type,
            nullable=container_prop.nullable,
            immutable=container_prop.immutable,
            auto_increment=container_prop.auto_increment,
            constraint_state=ConstraintOrIndexState(),
        )

    return ViewResponse(
        space=container.space,
        external_id=external_id,
        version="v1",
        properties=properties,
        last_updated_time=1,
        created_time=1,
        is_global=False,
        used_for="node",
        writable=True,
        queryable=True,
        description=None,
        name=None,
        filter=None,
        implements=None,
        mapped_containers=[container.as_id()],
    )


class TestViewLoader:
    def test_unchanged_view_int_version(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)
        raw_file = """- space: sp_space
  externalId: my_view
  version: 1"""
        file = MagicMock(spec=Path)
        file.read_text.return_value = raw_file
        cdf_view = ViewResponse(
            space="sp_space",
            external_id="my_view",
            version="1",
            last_updated_time=1,
            created_time=1,
            implements=None,
            writable=True,
            used_for="node",
            is_global=False,
            properties={},
            mapped_containers=[],
            queryable=True,
        )

        toolkit_client_approval.append(ViewResponse, [cdf_view])

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
                    (SpaceCRUD, SpaceReference(space="sp_my_space")),
                    (ContainerCRUD, ContainerReference(space="my_container_space", external_id="my_container")),
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
                    (SpaceCRUD, SpaceReference(space="sp_my_space")),
                    (ViewCRUD, ViewReference(space="my_view_space", external_id="my_view", version="1")),
                    (ViewCRUD, ViewReference(space="my_other_view_space", external_id="my_edge_view", version="42")),
                ],
                id="View with one container property",
            ),
        ],
    )
    def test_get_dependent_items(self, item: dict, expected: list[tuple[type[ResourceCRUD], Hashable]]) -> None:
        actual = ViewCRUD.get_dependent_items(item)

        assert list(actual) == expected

    @pytest.mark.parametrize(
        "view_ids,ordering_constraints,test_description",
        [
            pytest.param(
                [
                    ViewReference(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ViewReference(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                    ViewReference(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                ],
                [
                    (
                        ViewReference(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                        ViewReference(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                    ),
                    (
                        ViewReference(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                        ViewReference(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ),
                ],
                "Transitive chain: CogniteSourceSystem -> CogniteSourceable -> CogniteAsset",
                id="transitive_chain",
            ),
            pytest.param(
                [
                    ViewReference(space="cdf_cdm", external_id="CogniteActivity", version="v1"),
                    ViewReference(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                    ViewReference(space="cdf_cdm", external_id="CogniteSchedulable", version="v1"),
                    ViewReference(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                ],
                [
                    (
                        ViewReference(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                        ViewReference(space="cdf_cdm", external_id="CogniteSourceable", version="v1"),
                    ),
                    (
                        ViewReference(space="cdf_cdm", external_id="CogniteSchedulable", version="v1"),
                        ViewReference(space="cdf_cdm", external_id="CogniteActivity", version="v1"),
                    ),
                ],
                "Multiple independent chains: (CogniteSourceSystem -> CogniteSourceable) and (CogniteSchedulable -> CogniteActivity)",
                id="multiple_independent_chains",
            ),
            pytest.param(
                [
                    ViewReference(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ViewReference(space="cdf_cdm", external_id="CogniteAssetClass", version="v1"),
                    ViewReference(space="cdf_cdm", external_id="CogniteAssetType", version="v1"),
                    ViewReference(space="cdf_cdm", external_id="CogniteDescribable", version="v1"),
                ],
                [
                    (
                        ViewReference(space="cdf_cdm", external_id="CogniteDescribable", version="v1"),
                        ViewReference(space="cdf_cdm", external_id="CogniteAssetType", version="v1"),
                    ),
                    (
                        ViewReference(space="cdf_cdm", external_id="CogniteDescribable", version="v1"),
                        ViewReference(space="cdf_cdm", external_id="CogniteAssetClass", version="v1"),
                    ),
                    (
                        ViewReference(space="cdf_cdm", external_id="CogniteAssetClass", version="v1"),
                        ViewReference(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ),
                    (
                        ViewReference(space="cdf_cdm", external_id="CogniteAssetType", version="v1"),
                        ViewReference(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                    ),
                ],
                "Diamond: CogniteDescribable -> CogniteAssetType/CogniteAssetClass -> CogniteAsset",
                id="diamond_dependency",
            ),
        ],
    )
    def test_topological_sort_container_constraints_dependency_patterns(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cognite_core_no_3D: DataModelResponseWithViews,
        cognite_core_containers_no_3D: list[ContainerResponse],
        view_ids: list[ViewReference],
        ordering_constraints: list[tuple[ViewReference, ViewReference]],
        test_description: str,
    ) -> None:
        """Test various dependency patterns: transitive chains, independent chains, and diamond dependencies."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)
        toolkit_client_approval.append(ViewResponse, cognite_core_no_3D.views)
        toolkit_client_approval.append(ContainerResponse, cognite_core_containers_no_3D)

        sorted_views = loader.topological_sort_container_constraints(view_ids)

        # Verify same number of views returned
        assert len(sorted_views) == len(view_ids), (
            f"{test_description}: Expected {len(view_ids)} views, got {len(sorted_views)}"
        )

        # Verify all ordering constraints
        for before_view_id, after_view_id in ordering_constraints:
            before_idx = sorted_views.index(before_view_id)
            after_idx = sorted_views.index(after_view_id)
            assert before_idx < after_idx, (
                f"{test_description}: {before_view_id.external_id} should come before {after_view_id.external_id}"
            )

    def test_topological_sort_container_constraints_cyclical_dependency(
        self, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        """Test that cyclical dependencies raise ToolkitCycleError."""

        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)

        # Create three containers that form a cycle: A -> B -> C -> A
        container_a = _create_test_container(
            "ContainerA",
            {
                "refC": ContainerPropertyDefinition(
                    type=DirectNodeRelation(container=ContainerReference(space="my_space", external_id="ContainerC")),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )
        container_b = _create_test_container(
            "ContainerB",
            {
                "refA": ContainerPropertyDefinition(
                    type=DirectNodeRelation(container=ContainerReference(space="my_space", external_id="ContainerA")),
                    nullable=True,
                    immutable=False,
                    auto_increment=False,
                )
            },
        )
        container_c = _create_test_container(
            "ContainerC",
            {
                "name": ContainerPropertyDefinition(
                    type=TextProperty(), nullable=True, immutable=False, auto_increment=False
                ),
            },
            {
                "requiresB": RequiresConstraintDefinition(
                    require=ContainerReference(space="my_space", external_id="ContainerB")
                ),
            },
        )

        view_a = _create_test_view("ViewA", container_a)
        view_b = _create_test_view("ViewB", container_b)
        view_c = _create_test_view("ViewC", container_c)

        toolkit_client_approval.append(ViewResponse, [view_a, view_b, view_c])
        toolkit_client_approval.append(ContainerResponse, [container_a, container_b, container_c])

        view_ids = [view_a.as_id(), view_b.as_id(), view_c.as_id()]

        with pytest.raises(ToolkitCycleError):
            loader.topological_sort_container_constraints(view_ids)

    @pytest.mark.parametrize(
        "view_id,expected_readonly_props",
        [
            pytest.param(
                ViewReference(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                {"pathLastUpdatedTime", "path", "root"},
                id="CogniteAsset_has_readonly_properties",
            ),
            pytest.param(
                ViewReference(space="cdf_cdm", external_id="CogniteFile", version="v1"),
                {"isUploaded", "uploadedTime"},
                id="CogniteFile_has_readonly_properties",
            ),
            pytest.param(
                ViewReference(space="cdf_cdm", external_id="CogniteSourceSystem", version="v1"),
                set(),
                id="CogniteSourceSystem_no_readonly_properties",
            ),
        ],
    )
    def test_get_readonly_properties(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cognite_core_no_3D: DataModelResponseWithViews,
        view_id: ViewReference,
        expected_readonly_props: set[str],
    ) -> None:
        """Test that get_readonly_properties identifies readonly properties from containers."""
        loader = ViewCRUD.create_loader(toolkit_client_approval.mock_client)
        toolkit_client_approval.append(ViewResponse, cognite_core_no_3D.views)

        readonly_props = loader.get_readonly_properties(view_id)
        assert readonly_props == expected_readonly_props
