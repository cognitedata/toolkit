from pathlib import Path

import pytest
from cognite.client.data_classes.data_modeling import DataModelId, SpaceApply

from cognite_toolkit._cdf_tk.apps._dump_app import DumpConfigApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import DumpResourceCommand
from cognite_toolkit._cdf_tk.commands.dump_resource import DataModelFinder
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, DataModelCRUD, GraphQLCRUD, SpaceCRUD, ViewCRUD
from tests.data import NAUGHTY_PROJECT


@pytest.fixture()
def deployed_misbehaving_grandparent(toolkit_client: ToolkitClient) -> DataModelId:
    loader = GraphQLCRUD.create_loader(toolkit_client)
    filepaths = loader.find_files(NAUGHTY_PROJECT / MODULES / "difficult_graphql")
    assert len(filepaths) == 1
    model_list = loader.load_resource_file(filepaths[0])
    assert len(model_list) == 1
    model = loader.load_resource(model_list[0])
    toolkit_client.data_modeling.spaces.apply(SpaceApply(space=model.space))

    retrieved = loader.retrieve([loader.get_id(model)])
    if not retrieved:
        return loader.create([model])[0].as_id()
    return retrieved[0].as_id()


class TestDumpResource:
    def test_dump_model_without_version(self, toolkit_client: ToolkitClient, tmp_path: Path) -> None:
        DumpConfigApp().dump_datamodel_cmd(
            None,
            ["cdf_cdm", "CogniteCore"],
            tmp_path,
        )

        data_model_folder = tmp_path / DataModelCRUD.folder_name
        assert data_model_folder.exists()
        assert sum(1 for _ in data_model_folder.glob(f"*{DataModelCRUD.kind}.yaml")) == 1

    def test_dump_global_model(self, toolkit_client: ToolkitClient, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        cmd = DumpResourceCommand(silent=True)
        cmd.dump_to_yamls(
            DataModelFinder(toolkit_client, DataModelId("cdf_cdm", "CogniteCore", "v1")),
            output_dir=output_dir,
            clean=False,
            verbose=False,
        )

        data_model_folder = output_dir / DataModelCRUD.folder_name
        assert data_model_folder.exists()
        assert sum(1 for _ in data_model_folder.glob(f"*{DataModelCRUD.kind}.yaml")) == 1
        assert sum(1 for _ in data_model_folder.glob(f"**/*{ViewCRUD.kind}.yaml")) == 33
        assert sum(1 for _ in data_model_folder.glob(f"**/*{ContainerCRUD.kind}.yaml")) == 29
        assert sum(1 for _ in data_model_folder.glob(f"**/*{SpaceCRUD.kind}.yaml")) == 2

    def test_dump_misbehaving_grandparent(
        self, deployed_misbehaving_grandparent: DataModelId, toolkit_client: ToolkitClient, tmp_path: Path
    ) -> None:
        output_dir = tmp_path / "output"
        cmd = DumpResourceCommand(silent=True)
        cmd.dump_to_yamls(
            DataModelFinder(toolkit_client, deployed_misbehaving_grandparent),
            output_dir=output_dir,
            clean=False,
            verbose=False,
        )

        data_model_folder = output_dir / DataModelCRUD.folder_name
        assert data_model_folder.exists()
        view_loader = ViewCRUD.create_loader(toolkit_client)
        views_by_id = {
            view_loader.get_id(item).external_id: view_loader.load_resource(item)
            for filepath in view_loader.find_files(output_dir)
            for item in view_loader.load_resource_file(filepath)
        }

        assert "NumericProperty" in views_by_id
        view = views_by_id["NumericProperty"]
        assert [parent.external_id for parent in view.implements] == ["Property", "ScalarProperty"]
