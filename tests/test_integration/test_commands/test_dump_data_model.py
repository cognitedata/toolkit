from pathlib import Path

from cognite_toolkit._cdf_tk.apps import DumpApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.loaders import DataModelLoader


class TestDumpResource:
    def test_dump_model_without_version(self, toolkit_client: ToolkitClient, tmp_path: Path) -> None:
        DumpApp().dump_datamodel_cmd(
            None,
            ["cdf_cdm", "CogniteCore"],
            tmp_path,
        )

        data_model_folder = tmp_path / DataModelLoader.folder_name
        assert data_model_folder.exists()
        assert sum(1 for _ in data_model_folder.glob(f"*{DataModelLoader.kind}.yaml")) == 1
