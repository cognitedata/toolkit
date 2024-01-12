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
SNAPSHOTS_DIR.mkdir(exist_ok=True)


def test_describe_datamodel(
    cognite_client_approval: ApprovalCogniteClient,
    data_regression,
    file_regression,
    capfd,
    freezer,
):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.project = "test"
    cdf_tool.client = cognite_client_approval.mock_client
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client

    spaces = [
        Space.load(filepath.read_text())
        for filepath in SpaceLoader.create_loader(cdf_tool).get_matching_files(Path(DATA_FOLDER, "data_models"))
    ]
    data_models = [
        DataModel.load(filepath.read_text())
        for filepath in DataModelLoader.create_loader(cdf_tool).get_matching_files(Path(DATA_FOLDER, "data_models"))
    ]
    containers = [
        Container.load(filepath.read_text())
        for filepath in ContainerLoader.create_loader(cdf_tool).get_matching_files(Path(DATA_FOLDER, "data_models"))
    ]
    views = [
        View.load(filepath.read_text())
        for filepath in ViewLoader.create_loader(cdf_tool).get_matching_files(Path(DATA_FOLDER, "data_models"))
    ]
    cognite_client_approval.append(Space, spaces)
    cognite_client_approval.append(Container, containers)
    cognite_client_approval.append(View, views)
    cognite_client_approval.append(DataModel, data_models)

    describe_datamodel(cdf_tool, "test", "test")
    out, _ = capfd.readouterr()
    file_regression.check(out, encoding="utf-8", fullpath=SNAPSHOTS_DIR / "describe_datamodel.txt")

    dump = cognite_client_approval.dump()
    assert dump == {}
    calls = cognite_client_approval.retrieve_calls()
    data_regression.check(calls, fullpath=SNAPSHOTS_DIR / "describe_datamodel.yaml")
