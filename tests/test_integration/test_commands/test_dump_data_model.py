from pathlib import Path

from cognite.client.data_classes.data_modeling import DataModelId

from cognite_toolkit._cdf_tk.apps._dump_app import DumpConfigApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import DumpResourceCommand
from cognite_toolkit._cdf_tk.commands.dump_resource import DataModelFinder
from cognite_toolkit._cdf_tk.loaders import ContainerLoader, DataModelLoader, SpaceLoader, ViewLoader


class TestDumpResource:
    def test_dump_model_without_version(self, toolkit_client: ToolkitClient, tmp_path: Path) -> None:
        DumpConfigApp().dump_datamodel_cmd(
            None,
            ["cdf_cdm", "CogniteCore"],
            tmp_path,
        )

        data_model_folder = tmp_path / DataModelLoader.folder_name
        assert data_model_folder.exists()
        assert sum(1 for _ in data_model_folder.glob(f"*{DataModelLoader.kind}.yaml")) == 1

    def test_dump_global_model(self, toolkit_client: ToolkitClient, tmp_path: Path) -> None:
        output_dir = tmp_path / "output"
        cmd = DumpResourceCommand(silent=True)
        cmd.dump_to_yamls(
            DataModelFinder(toolkit_client, DataModelId("cdf_cdm", "CogniteCore", "v1")),
            output_dir=output_dir,
            clean=False,
            verbose=False,
        )

        data_model_folder = output_dir / DataModelLoader.folder_name
        assert data_model_folder.exists()
        assert sum(1 for _ in data_model_folder.glob(f"*{DataModelLoader.kind}.yaml")) == 1
        assert sum(1 for _ in data_model_folder.glob(f"**/*{ViewLoader.kind}.yaml")) == 33
        assert sum(1 for _ in data_model_folder.glob(f"**/*{ContainerLoader.kind}.yaml")) == 29
        assert sum(1 for _ in data_model_folder.glob(f"**/*{SpaceLoader.kind}.yaml")) == 2
