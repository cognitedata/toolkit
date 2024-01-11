import re
from pathlib import Path
from unittest.mock import MagicMock

from cognite_toolkit.cdf_tk.describe import describe_datamodel
from cognite_toolkit.cdf_tk.load import (
    Container,
    ContainerLoader,
    DataModel,
    DataModelLoader,
    Space,
    SpaceLoader,
    View,
    ViewLoader,
)
from cognite_toolkit.cdf_tk.utils import CDFToolConfig
from tests.conftest import ApprovalCogniteClient

THIS_FOLDER = Path(__file__).resolve().parent

DATA_FOLDER = THIS_FOLDER / "describe_data"
SNAPSHOTS_DIR = THIS_FOLDER / "describe_data_snapshots"


def test_describe_datamodel(cognite_client_approval: ApprovalCogniteClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.client = cognite_client_approval.mock_client
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client

    space_loader = SpaceLoader.create_loader(cdf_tool)
    data_model_loader = DataModelLoader.create_loader(cdf_tool)
    container_loader = ContainerLoader.create_loader(cdf_tool)
    view_loader = ViewLoader.create_loader(cdf_tool)
    spaces = [
        Space.load(filepath.read_text())
        for filepath in [
            file
            for type_ in space_loader.filetypes
            for file in Path(DATA_FOLDER, "data_models").glob(f"**/*.{type_}")
            if re.compile(space_loader.filename_pattern).match(file.stem)
        ]
    ]
    data_models = [
        DataModel.load(filepath.read_text())
        for filepath in [
            file
            for type_ in data_model_loader.filetypes
            for file in Path(DATA_FOLDER, "data_models").glob(f"**/*.{type_}")
            if re.compile(data_model_loader.filename_pattern).match(file.stem)
        ]
    ]
    containers = [
        Container.load(filepath.read_text())
        for filepath in [
            file
            for type_ in container_loader.filetypes
            for file in Path(DATA_FOLDER, "data_models").glob(f"**/*.{type_}")
            if re.compile(container_loader.filename_pattern).match(file.stem)
        ]
    ]
    views = View.load(Path(DATA_FOLDER, "data_models", "4.Asset.view.yaml").read_text())
    views = [
        View.load(filepath.read_text())
        for filepath in [
            file
            for type_ in view_loader.filetypes
            for file in Path(DATA_FOLDER, "data_models").glob(f"**/*.{type_}")
            if re.compile(view_loader.filename_pattern).match(file.stem)
        ]
    ]
    cognite_client_approval.append(Space, spaces)
    cognite_client_approval.append(Container, containers)
    cognite_client_approval.append(View, views)
    cognite_client_approval.append(DataModel, data_models)

    describe_datamodel(cdf_tool, "test", "test")
