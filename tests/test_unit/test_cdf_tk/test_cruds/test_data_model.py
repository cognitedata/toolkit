from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, NodeId, SpaceId, ViewDirectId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    DataModelRequest,
    DataModelResponse,
    ViewId,
    ViewRequest,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling._view_property import (
    SingleEdgeProperty,
    SingleReverseDirectRelationPropertyRequest,
    ViewCorePropertyRequest,
)
from cognite_toolkit._cdf_tk.client.resource_classes.graphql_data_model import GraphQLDataModelResponse
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.constants import VIEW_UPSERT_BATCH_LIMIT
from cognite_toolkit._cdf_tk.exceptions import ToolkitCycleError
from cognite_toolkit._cdf_tk.resources_ios import DataModelIO, EdgeCRUD, NodeCRUD, ResourceWorker, SpaceCRUD
from cognite_toolkit._cdf_tk.resources_ios._resource_ios import GraphQLCRUD, ViewIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDataModelLoader:
    def test_update_data_model_random_view_order(
        self, env_vars_with_client: EnvironmentVariables, toolkit_client_approval: ApprovalToolkitClient
    ):
        cdf_data_model = DataModelResponse(
            space="sp_space",
            external_id="my_model",
            version="1",
            views=[
                ViewId(space="sp_space", external_id="first", version="1"),
                ViewId(space="sp_space", external_id="second", version="1"),
            ],
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            is_global=False,
        )
        # Simulating that the data model is available in CDF
        toolkit_client_approval.append(DataModelResponse, cdf_data_model)

        local_data_model = DataModelRequest(
            space="sp_space",
            external_id="my_model",
            version="1",
            views=[
                ViewId(space="sp_space", external_id="second", version="1"),
                ViewId(space="sp_space", external_id="first", version="1"),
            ],
            description=None,
            name=None,
        ).dump_yaml()

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_data_model

        loader = DataModelIO.create_loader(
            env_vars_with_client.get_client(),
        )
        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([filepath])

        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}

    def test_are_equal_version_int(self, env_vars_with_client: EnvironmentVariables) -> None:
        local_yaml = """space: sp_space
externalId: my_model
version: 1
views:
  - space: sp_space
    externalId: first
    version: 1
    type: view
        """
        cdf_data_model = DataModelResponse(
            space="sp_space",
            external_id="my_model",
            version="1",
            views=[ViewId(space="sp_space", external_id="first", version="1")],
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            is_global=False,
        )
        loader = DataModelIO.create_loader(env_vars_with_client.get_client())
        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_yaml
        # The load filepath method ensures version is read as an int.
        local_dict = loader.load_resource_file(filepath, {})[0]

        cdf_dumped = loader.dump_resource(cdf_data_model, local_dict)

        assert local_dict == cdf_dumped


class TestGraphQLLoader:
    def test_deployment_order(
        self, env_vars_with_client: EnvironmentVariables, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        loader = GraphQLCRUD.create_loader(env_vars_with_client.get_client())
        # The first model is dependent on the second model
        first_file = self._create_mock_file(
            """
type WindTurbine @import @view(space: "second_space", externalId: "GeneratingUnit", version: "v1"){
name: String}""",
            "first_space",
            "WindTurbineModel",
        )
        second_file = self._create_mock_file(
            """
type GeneratingUnit {
        name: String
            }""",
            "second_space",
            "GeneratingUnitModel",
        )

        items = loader.load_resource_file(first_file, {})
        items.extend(loader.load_resource_file(second_file, {}))

        resources = [loader.load_resource(item) for item in items]

        loader.create(resources)

        created = toolkit_client_approval.created_resources_of_type(GraphQLDataModelResponse)

        assert len(created) == 2
        assert created[0].external_id == "GeneratingUnitModel"
        assert created[1].external_id == "WindTurbineModel"

    def test_raise_cycle_error(
        self, env_vars_with_client: EnvironmentVariables, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        loader = GraphQLCRUD.create_loader(env_vars_with_client.get_client())
        # The two models are dependent on each other
        first_file = self._create_mock_file(
            """type WindTurbine @import(dataModel: {externalId: "SolarModel", version: "v1", space: "second_space"}) {
name: String}""",
            "first_space",
            "WindTurbineModel",
        )
        second_file = self._create_mock_file(
            """type Solar @import(dataModel: {externalId: "WindTurbineModel", version: "v1", space: "first_space"}) {
        name: String
            }""",
            "second_space",
            "SolarModel",
        )

        items = loader.load_resource_file(first_file, {})
        items.extend(loader.load_resource_file(second_file, {}))
        resources = [loader.load_resource(item) for item in items]
        with pytest.raises(ToolkitCycleError) as e:
            loader.create(resources)

        assert "Cycle detected" in str(e.value)
        assert [m.external_id for m in e.value.args[1]] == [
            "WindTurbineModel",
            "SolarModel",
            "WindTurbineModel",
        ]

    def test_load_version_int(self, env_vars_with_client: EnvironmentVariables) -> None:
        file = self._create_mock_file(
            """type WindTurbine{
            name: String}""",
            "DG-COR-ALL-DMD",
            "AssetHierarchyDOM",
            "3_0_2",
        )
        loader = GraphQLCRUD.create_loader(env_vars_with_client.get_client())

        items = loader.load_resource_file(file, {})

        assert len(items) == 1
        resource = loader.load_resource(items[0], is_dry_run=False)
        assert resource.version == "3_0_2"

    @staticmethod
    def _create_mock_file(model: str, space: str, external_id: str, version: int | str = "v1") -> MagicMock:
        yaml_file = MagicMock(spec=Path)
        yaml_file.read_text.return_value = f"""space: {space}
externalId: {external_id}
version: {version}
"""
        yaml_file.stem = f"{external_id}.GraphQLSchema"

        graphql_file = MagicMock(spec=Path)
        graphql_file.read_text.return_value = model
        graphql_file.name = f"{external_id}.graphql"
        graphql_file.is_file.return_value = True

        yaml_file.parent = MagicMock(spec=Path)
        yaml_file.parent.__truediv__ = MagicMock(return_value=graphql_file)
        return yaml_file


@pytest.fixture()
def parent_grandparent_view() -> list[ViewResponse]:
    return [
        ViewResponse(
            space="space",
            external_id="Parent",
            version="v1",
            name="Parent",
            description=None,
            implements=[ViewId(space="space", external_id="GrandParent", version="v1")],
            properties={},
            last_updated_time=1,
            created_time=1,
            filter=None,
            writable=True,
            used_for="node",
            is_global=False,
            mapped_containers=[],
            queryable=False,
        ),
        ViewResponse(
            space="space",
            external_id="GrandParent",
            version="v1",
            name="GrandParent",
            description=None,
            implements=[],
            properties={},
            last_updated_time=1,
            created_time=1,
            filter=None,
            writable=True,
            used_for="node",
            is_global=False,
            mapped_containers=[],
            queryable=False,
        ),
    ]


class TestViewLoader:
    def test_topological_sorting(self, parent_grandparent_view: list[ViewResponse]) -> None:
        with monkeypatch_toolkit_client() as client:
            client.tool.views.retrieve.return_value = parent_grandparent_view
            parent = ViewId(space="space", external_id="Parent", version="v1")
            grandparent = ViewId(space="space", external_id="GrandParent", version="v1")
            loader = ViewIO(client, Path("build_dir"), None, topological_sort_implements=True)
            actual = loader.topological_sort_implements(
                [
                    parent,
                    grandparent,
                ]
            )

        assert actual == [grandparent, parent]

    def test_topological_sorting_cycle(self, parent_grandparent_view: list[ViewResponse]) -> None:
        parent_grandparent_view[1] = parent_grandparent_view[1].model_copy(
            update={"implements": [parent_grandparent_view[0].as_id()]}
        )
        parent = ViewId(space="space", external_id="Parent", version="v1")
        grandparent = ViewId(space="space", external_id="GrandParent", version="v1")

        with monkeypatch_toolkit_client() as client, pytest.raises(ToolkitCycleError) as exc_info:
            client.tool.views.retrieve.return_value = parent_grandparent_view
            loader = ViewIO(client, Path("build_dir"), None, topological_sort_implements=True)
            loader.topological_sort_implements(
                [
                    parent,
                    grandparent,
                ]
            )

        assert "cycle in implements" in str(exc_info.value)


class TestViewDeployTopologicalSort:
    @pytest.mark.parametrize(
        "dependency_property",
        [
            pytest.param(
                SingleReverseDirectRelationPropertyRequest(
                    source=ViewId(space="sp_space", external_id="Other", version="v1"),
                    through=ViewDirectId(
                        source=ViewId(space="sp_space", external_id="Dependency", version="v1"),
                        identifier="direct_prop",
                    ),
                ),
                id="reverse_direct_relation_through",
            ),
            pytest.param(
                SingleReverseDirectRelationPropertyRequest(
                    source=ViewId(space="sp_space", external_id="Dependency", version="v1"),
                    through=ViewDirectId(
                        source=ViewId(space="sp_space", external_id="Other", version="v1"),
                        identifier="direct_prop",
                    ),
                ),
                id="reverse_direct_relation_source",
            ),
            pytest.param(
                ViewCorePropertyRequest(
                    container=ContainerId(space="sp_space", external_id="some_container"),
                    container_property_identifier="ref",
                    source=ViewId(space="sp_space", external_id="Dependency", version="v1"),
                ),
                id="direct_relation_source",
            ),
            pytest.param(
                SingleEdgeProperty(
                    source=ViewId(space="sp_space", external_id="Dependency", version="v1"),
                    type=NodeId(space="sp_space", external_id="edge_type"),
                ),
                id="edge_connection_source",
            ),
            pytest.param(
                SingleEdgeProperty(
                    source=ViewId(space="sp_space", external_id="Other", version="v1"),
                    type=NodeId(space="sp_space", external_id="edge_type"),
                    edge_source=ViewId(space="sp_space", external_id="Dependency", version="v1"),
                ),
                id="edge_connection_edge_source",
            ),
        ],
    )
    def test_property_dependency_ordering(self, dependency_property: ViewCorePropertyRequest) -> None:
        dependent_view = ViewRequest(
            space="sp_space",
            external_id="Dependent",
            version="v1",
            properties={"prop": dependency_property},
        )
        dependency_view = ViewRequest(space="sp_space", external_id="Dependency", version="v1")

        with monkeypatch_toolkit_client() as client:
            loader = ViewIO(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches([dependent_view, dependency_view])

        flat_ids = [view.external_id for batch in batches for view in batch]
        assert flat_ids.index("Dependency") < flat_ids.index("Dependent")

    def test_large_scc_kept_in_single_batch(self) -> None:
        view_count = VIEW_UPSERT_BATCH_LIMIT + 25
        views = [
            ViewRequest(
                space="sp_space",
                external_id=f"View_{i}",
                version="v1",
                properties={
                    "ref": ViewCorePropertyRequest(
                        container=ContainerId(space="sp_space", external_id="some_container"),
                        container_property_identifier="ref",
                        source=ViewId(
                            space="sp_space",
                            external_id=f"View_{(i + 1) % view_count}",  # Cyclic dependency across all views
                            version="v1",
                        ),
                    )
                },
            )
            for i in range(view_count)
        ]

        with monkeypatch_toolkit_client() as client:
            loader = ViewIO(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches(views)

        assert len(batches) == 1, "All views in one SCC should stay in a single batch"
        assert len(batches[0]) == view_count

    def test_cycle_in_implements_raises(self) -> None:
        view_a = ViewRequest(
            space="sp_space",
            external_id="A",
            version="v1",
            implements=[ViewId(space="sp_space", external_id="B", version="v1")],
        )
        view_b = ViewRequest(
            space="sp_space",
            external_id="B",
            version="v1",
            implements=[ViewId(space="sp_space", external_id="A", version="v1")],
        )

        with monkeypatch_toolkit_client() as client:
            loader = ViewIO(client, Path("build_dir"), None)
            with pytest.raises(ToolkitCycleError):
                loader._compute_deploy_batches([view_a, view_b])


class TestDataModelCRUDGetDependencies:
    """Test get_dependencies method for DataModelCRUD."""

    def test_datamodel_with_space_only(self) -> None:
        """Test DataModel with no view dependencies."""
        from cognite_toolkit._cdf_tk.yaml_classes.data_model import DataModelYAML

        data_model = DataModelYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_model",
                "version": "1",
                "description": "A simple data model",
            }
        )

        deps = list(DataModelIO.get_dependencies(data_model))
        assert len(deps) == 1
        assert deps[0] == (SpaceCRUD, SpaceId(space="my_space"))

    def test_datamodel_with_views(self) -> None:
        """Test DataModel with view dependencies."""
        from cognite_toolkit._cdf_tk.yaml_classes.data_model import DataModelYAML

        data_model = DataModelYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_model",
                "version": "1",
                "views": [
                    {"type": "view", "space": "my_space", "externalId": "view1", "version": "1"},
                    {"type": "view", "space": "other_space", "externalId": "view2", "version": "1"},
                ],
            }
        )

        deps = list(DataModelIO.get_dependencies(data_model))
        assert len(deps) == 3
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (ViewIO, ViewId(space="my_space", external_id="view1", version="1")) in deps
        assert (ViewIO, ViewId(space="other_space", external_id="view2", version="1")) in deps


class TestNodeCRUDGetDependencies:
    """Test get_dependencies method for NodeCRUD."""

    def test_node_with_space_only(self) -> None:
        """Test Node with no view source dependencies."""
        from cognite_toolkit._cdf_tk.yaml_classes.instance import NodeYAML

        node = NodeYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_node",
            }
        )

        deps = list(NodeCRUD.get_dependencies(node))
        assert len(deps) == 1
        assert deps[0] == (SpaceCRUD, SpaceId(space="my_space"))

    def test_node_with_view_sources(self) -> None:
        """Test Node with view source dependencies."""
        from cognite_toolkit._cdf_tk.yaml_classes.instance import NodeYAML

        node = NodeYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_node",
                "sources": [
                    {
                        "source": {
                            "type": "view",
                            "space": "source_space",
                            "externalId": "source_view",
                            "version": "1",
                        },
                        "properties": {},
                    },
                ],
            }
        )

        deps = list(NodeCRUD.get_dependencies(node))
        assert len(deps) == 2
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (ViewIO, ViewId(space="source_space", external_id="source_view", version="1")) in deps


class TestEdgeCRUDGetDependencies:
    """Test get_dependencies method for EdgeCRUD."""

    def test_edge_with_space_only(self) -> None:
        """Test Edge with no view source dependencies."""
        from cognite_toolkit._cdf_tk.yaml_classes.instance import EdgeYAML

        edge = EdgeYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_edge",
                "type": {"space": "type_space", "externalId": "edge_type"},
                "startNode": {"space": "node_space", "externalId": "start_node"},
                "endNode": {"space": "node_space", "externalId": "end_node"},
            }
        )

        deps = list(EdgeCRUD.get_dependencies(edge))
        assert len(deps) == 4
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (NodeCRUD, NodeId(space="node_space", external_id="start_node")) in deps
        assert (NodeCRUD, NodeId(space="node_space", external_id="end_node")) in deps
        assert (NodeCRUD, NodeId(space="type_space", external_id="edge_type")) in deps

    def test_edge_with_view_sources(self) -> None:
        """Test Edge with view source dependencies."""
        from cognite_toolkit._cdf_tk.yaml_classes.instance import EdgeYAML

        edge = EdgeYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_edge",
                "type": {"space": "type_space", "externalId": "edge_type"},
                "startNode": {"space": "node_space", "externalId": "start_node"},
                "endNode": {"space": "node_space", "externalId": "end_node"},
                "sources": [
                    {
                        "source": {
                            "type": "view",
                            "space": "source_space",
                            "externalId": "source_view",
                            "version": "1",
                        },
                        "properties": {},
                    },
                ],
            }
        )

        deps = list(EdgeCRUD.get_dependencies(edge))
        assert len(deps) == 5
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (ViewIO, ViewId(space="source_space", external_id="source_view", version="1")) in deps
        assert (NodeCRUD, NodeId(space="node_space", external_id="start_node")) in deps
        assert (NodeCRUD, NodeId(space="node_space", external_id="end_node")) in deps
        assert (NodeCRUD, NodeId(space="type_space", external_id="edge_type")) in deps

    def test_edge_with_node_references(self) -> None:
        """Test Edge with node start/end references."""
        from cognite_toolkit._cdf_tk.yaml_classes.instance import EdgeYAML

        edge = EdgeYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_edge",
                "type": {"space": "type_space", "externalId": "edge_type"},
                "startNode": {"space": "node_space", "externalId": "start_node"},
                "endNode": {"space": "node_space", "externalId": "end_node"},
            }
        )

        deps = list(EdgeCRUD.get_dependencies(edge))
        assert len(deps) == 4
        assert (SpaceCRUD, SpaceId(space="my_space")) in deps
        assert (NodeCRUD, NodeId(space="node_space", external_id="start_node")) in deps
        assert (NodeCRUD, NodeId(space="node_space", external_id="end_node")) in deps
        assert (NodeCRUD, NodeId(space="type_space", external_id="edge_type")) in deps


class TestGraphQLCRUDGetDependencies:
    """Test get_dependencies method for GraphQLCRUD."""

    def test_graphql_with_space_only(self) -> None:
        """Test GraphQL data model with only space dependency."""
        from cognite_toolkit._cdf_tk.yaml_classes.graphql_model import GraphQLDataModelYAML

        graphql_model = GraphQLDataModelYAML.model_validate(
            {
                "space": "my_space",
                "externalId": "my_graphql_model",
                "version": "1",
                "description": "A GraphQL data model",
            }
        )

        deps = list(GraphQLCRUD.get_dependencies(graphql_model))
        assert len(deps) == 1
        assert deps[0] == (SpaceCRUD, SpaceId(space="my_space"))
