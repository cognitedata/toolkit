from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client import CogniteClient
from cognite.client.data_classes import DataSet

from cognite_toolkit.cdf_tk.load import (
    DatapointsLoader,
    DataSetsLoader,
    FileLoader,
    Loader,
    deploy_or_clean_resources,
)
from cognite_toolkit.cdf_tk.utils import CDFToolConfig

THIS_FOLDER = Path(__file__).resolve().parent

DATA_FOLDER = THIS_FOLDER / "load_data"
SNAPSHOTS_DIR = THIS_FOLDER / "load_data_snapshots"


@pytest.mark.parametrize(
    "loader_cls, directory",
    [
        (FileLoader, DATA_FOLDER / "files"),
        (DatapointsLoader, DATA_FOLDER / "timeseries_datapoints"),
    ],
)
def test_loader_class(
    loader_cls: type[Loader], directory: Path, cognite_client_approval: CogniteClient, data_regression
):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.verify_client.return_value = cognite_client_approval
    cdf_tool.verify_capabilities.return_value = cognite_client_approval
    cdf_tool.data_set_id = 999

    deploy_or_clean_resources(
        loader_cls.create_loader(cdf_tool), directory, cdf_tool, drop=False, action="deploy", dry_run=False
    )

    dump = cognite_client_approval.dump()
    data_regression.check(dump, fullpath=SNAPSHOTS_DIR / f"{directory.name}.yaml")


def test_upsert_data_set(cognite_client_approval: CogniteClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.verify_client.return_value = cognite_client_approval
    cdf_tool.verify_capabilities.return_value = cognite_client_approval

    loader = DataSetsLoader.create_loader(cdf_tool)
    loaded = loader.load_resource(DATA_FOLDER / "data_sets" / "1.my_datasets.yaml", dry_run=False)
    assert len(loaded) == 2

    first = DataSet.load(loaded[0].dump())
    # Set the properties that are set on the server side
    first.id = 42
    first.created_time = 42
    first.last_updated_time = 42
    # Simulate that the data set is already in CDF
    cognite_client_approval.data_sets.append(first)

    changed = loader.remove_unchanged(loaded)

    assert len(changed) == 1
