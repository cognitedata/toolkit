import platform
import re
from pathlib import Path
from unittest.mock import MagicMock

from cognite.client.data_classes.data_modeling import Container, DataModel, Space, View

from cognite_toolkit._cdf_tk.commands import DescribeCommand
from cognite_toolkit._cdf_tk.loaders import (
    ContainerLoader,
    DataModelLoader,
    SpaceLoader,
    ViewLoader,
)
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.data import DESCRIPTIONS_FOLDER
from tests.test_unit.conftest import ApprovalToolkitClient
from tests.test_unit.test_cdf_tk.constants import SNAPSHOTS_DIR_ALL

SNAPSHOTS_DIR = SNAPSHOTS_DIR_ALL / "describe_data_snapshots"
SNAPSHOTS_DIR.mkdir(exist_ok=True)


def test_describe_datamodel(
    toolkit_client_approval: ApprovalToolkitClient,
    data_regression,
    file_regression,
    capfd,
    freezer,
):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.project = "test"
    cdf_tool.client = toolkit_client_approval.mock_client
    cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client

    space_loader = SpaceLoader.create_loader(cdf_tool, None)
    data_model_loader = DataModelLoader.create_loader(cdf_tool, None)
    container_loader = ContainerLoader.create_loader(cdf_tool, None)
    view_loader = ViewLoader.create_loader(cdf_tool, None)
    spaces = [
        Space.load(filepath.read_text())
        for filepath in [
            file
            for type_ in space_loader.filetypes
            for file in Path(DESCRIPTIONS_FOLDER, "data_models").glob(f"**/*.{type_}")
            if re.compile(space_loader.filename_pattern).match(file.stem)
        ]
    ]
    data_models = [
        DataModel.load(filepath.read_text())
        for filepath in [
            file
            for type_ in data_model_loader.filetypes
            for file in Path(DESCRIPTIONS_FOLDER, "data_models").glob(f"**/*.{type_}")
            if re.compile(data_model_loader.filename_pattern).match(file.stem)
        ]
    ]
    containers = [
        Container.load(filepath.read_text())
        for filepath in [
            file
            for type_ in container_loader.filetypes
            for file in Path(DESCRIPTIONS_FOLDER, "data_models").glob(f"**/*.{type_}")
            if re.compile(container_loader.filename_pattern).match(file.stem)
        ]
    ]
    views = [
        View.load(filepath.read_text())
        for filepath in [
            file
            for type_ in view_loader.filetypes
            for file in Path(DESCRIPTIONS_FOLDER, "data_models").glob(f"**/*.{type_}")
            if re.compile(view_loader.filename_pattern).match(file.stem)
        ]
    ]
    toolkit_client_approval.append(Space, spaces)
    toolkit_client_approval.append(Container, containers)
    toolkit_client_approval.append(View, views)
    toolkit_client_approval.append(DataModel, data_models)

    DescribeCommand().execute(cdf_tool, "test", "test")
    out, _ = capfd.readouterr()
    # Strip trailing spaces
    out = "\n".join([line.rstrip() for line in out.splitlines()])
    if platform.system() == "Windows":
        # Windows console use different characters for tables in rich.
        fullpath = SNAPSHOTS_DIR / "describe_datamodel_windows.txt"
    else:
        fullpath = SNAPSHOTS_DIR / "describe_datamodel.txt"
    file_regression.check(out, encoding="utf-8", fullpath=fullpath)

    dump = toolkit_client_approval.dump()
    assert dump == {}
    calls = toolkit_client_approval.retrieve_calls()
    data_regression.check(calls, fullpath=SNAPSHOTS_DIR / "describe_datamodel.yaml")
