from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes import data_modeling as dm

from cognite_toolkit._cdf_tk.client.data_classes.graphql_data_models import GraphQLDataModel, GraphQLDataModelWriteList
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.cruds import DataModelCRUD, ResourceWorker
from cognite_toolkit._cdf_tk.cruds._resource_cruds import GraphQLCRUD, ViewCRUD
from cognite_toolkit._cdf_tk.exceptions import ToolkitCycleError
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDataModelLoader:
    def test_update_data_model_random_view_order(
        self, env_vars_with_client: EnvironmentVariables, toolkit_client_approval: ApprovalToolkitClient
    ):
        cdf_data_model = dm.DataModel(
            space="sp_space",
            external_id="my_model",
            version="1",
            views=[
                dm.ViewId(space="sp_space", external_id="first", version="1"),
                dm.ViewId(space="sp_space", external_id="second", version="1"),
            ],
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            is_global=False,
        )
        # Simulating that the data model is available in CDF
        toolkit_client_approval.append(dm.DataModel, cdf_data_model)

        local_data_model = dm.DataModelApply(
            space="sp_space",
            external_id="my_model",
            version="1",
            views=[
                dm.ViewId(space="sp_space", external_id="second", version="1"),
                dm.ViewId(space="sp_space", external_id="first", version="1"),
            ],
            description=None,
            name=None,
        ).dump_yaml()

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_data_model

        loader = DataModelCRUD.create_loader(
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
        cdf_data_model = dm.DataModel(
            space="sp_space",
            external_id="my_model",
            version="1",
            views=[dm.ViewId(space="sp_space", external_id="first", version="1")],
            last_updated_time=1,
            created_time=1,
            description=None,
            name=None,
            is_global=False,
        )
        loader = DataModelCRUD.create_loader(env_vars_with_client.get_client())
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

        resources = GraphQLDataModelWriteList([loader.load_resource(item) for item in items])

        loader.create(resources)

        created = toolkit_client_approval.created_resources_of_type(GraphQLDataModel)

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
        resources = GraphQLDataModelWriteList([loader.load_resource(item) for item in items])
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
dml: model.graphql
"""

        graphql_file = MagicMock(spec=Path)
        graphql_file.read_text.return_value = model
        graphql_file.name = "model.graphql"
        graphql_file.is_file.return_value = True

        yaml_file.with_suffix.return_value = graphql_file
        return yaml_file


@pytest.fixture()
def parent_grandparent_view() -> dm.ViewlList:
    return dm.ViewList(
        [
            dm.View(
                space="space",
                external_id="Parent",
                version="v1",
                name="Parent",
                description=None,
                implements=[dm.ViewId("space", "GrandParent", "v1")],
                properties={},
                last_updated_time=1,
                created_time=1,
                filter=None,
                writable=True,
                used_for="node",
                is_global=False,
            ),
            dm.View(
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
            ),
        ]
    )


class TestViewLoader:
    def test_topological_sorting(self, parent_grandparent_view: dm.ViewList) -> None:
        with monkeypatch_toolkit_client() as client:
            client.data_modeling.views.retrieve.return_value = parent_grandparent_view
            loader = ViewCRUD(client, Path("build_dir"), None, topological_sort_implements=True)
            actual = loader.topological_sort(
                [dm.ViewId("space", "Parent", "v1"), dm.ViewId("space", "GrandParent", "v1")]
            )

        assert actual == [dm.ViewId("space", "GrandParent", "v1"), dm.ViewId("space", "Parent", "v1")]

    def test_topological_sorting_cycle(self, parent_grandparent_view: dm.ViewList) -> None:
        parent_grandparent_view[1].implements = [parent_grandparent_view[0].as_id()]

        with monkeypatch_toolkit_client() as client, pytest.raises(ToolkitCycleError) as exc_info:
            client.data_modeling.views.retrieve.return_value = parent_grandparent_view
            loader = ViewCRUD(client, Path("build_dir"), None, topological_sort_implements=True)
            loader.topological_sort([dm.ViewId("space", "Parent", "v1"), dm.ViewId("space", "GrandParent", "v1")])

        assert "cycle in implements" in str(exc_info.value)
