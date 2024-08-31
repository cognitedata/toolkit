from pathlib import Path
from unittest.mock import MagicMock

from cognite.client.data_classes import data_modeling as dm

from cognite_toolkit._cdf_tk.commands import DeployCommand
from cognite_toolkit._cdf_tk.loaders import DataModelLoader
from cognite_toolkit._cdf_tk.loaders._resource_loaders import GraphQLLoader
from cognite_toolkit._cdf_tk.loaders.data_classes import GraphQLDataModel
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDataModelLoader:
    def test_update_data_model_random_view_order(
        self, cdf_tool_config: CDFToolConfig, toolkit_client_approval: ApprovalToolkitClient
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
        )

        loader = DataModelLoader.create_loader(cdf_tool_config, None)
        cmd = DeployCommand(print_warning=False)
        to_create, to_change, unchanged = cmd.to_create_changed_unchanged_triple(
            dm.DataModelApplyList([local_data_model]), loader
        )

        assert len(to_create) == 0
        assert len(to_change) == 0
        assert len(unchanged) == 1

    def test_are_equal_version_int(self, cdf_tool_config: CDFToolConfig) -> None:
        local_data_model = dm.DataModelApply.load("""space: sp_space
externalId: my_model
version: 1
views:
  - space: sp_space
    externalId: first
    version: 1
    type: view
        """)
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
        loader = DataModelLoader.create_loader(cdf_tool_config, None)

        are_equal, local_dumped, cdf_dumped = loader.are_equal(local_data_model, cdf_data_model, return_dumped=True)

        assert local_dumped == cdf_dumped


class TestGraphQLLoader:
    def test_deployment_order(
        self, cdf_tool_config: CDFToolConfig, toolkit_client_approval: ApprovalToolkitClient
    ) -> None:
        loader = GraphQLLoader.create_loader(cdf_tool_config, None)
        # The first model is dependent on the second model
        first_file = self._create_mock_file(
            """
type WindTurbine @import @view(space: "second_space", externalId: "GeneratingUnit", version: "v1") @import{
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
            "GeneratingUnit",
        )

        items = loader.load_resource(first_file, cdf_tool_config, skip_validation=True)
        items.extend(loader.load_resource(second_file, cdf_tool_config, skip_validation=True))

        loader.create(items)

        created = toolkit_client_approval.created_resources_of_type(GraphQLDataModel)

        assert len(created) == 2
        assert created[0].external_id == "GeneratingUnit"
        assert created[1].external_id == "WindTurbineModel"

    def test_raise_cycle_error(
        self, cdf_tool_config: CDFToolConfig, toolkit_client_approval: ApprovalToolkitClient
    ) -> None: ...

    @staticmethod
    def _create_mock_file(first_model: str, space: str, external_id: str):
        first_file = MagicMock(spec=Path)
        first_file.read_text.return_value = f"""space: {space}
externalId: {external_id}
version: v1
dml: model.graphql
"""
        first_model_file = MagicMock(spec=Path)
        first_model_file.read_text.return_value = first_model
        first_model_file.name = "model.graphql"
        first_model_file.is_file.return_value = True
        first_file.parent.iterdir.return_value = [first_model_file]
        return first_file
