from pathlib import Path
from unittest.mock import MagicMock

import pytest
import yaml
from cognite.client.data_classes import DataSet
from pytest import MonkeyPatch

from cognite_toolkit.cdf_tk.load import (
    AuthLoader,
    DatapointsLoader,
    DataSetsLoader,
    FileMetadataLoader,
    ResourceLoader,
)
from cognite_toolkit.cdf_tk.utils import CDFToolConfig
from tests.approval_client import ApprovalCogniteClient
from tests.utils import mock_read_yaml_file

THIS_FOLDER = Path(__file__).resolve().parent

DATA_FOLDER = THIS_FOLDER / "load_data"
SNAPSHOTS_DIR = THIS_FOLDER / "load_data_snapshots"


@pytest.mark.parametrize(
    "loader_cls, directory",
    [
        (FileMetadataLoader, DATA_FOLDER / "files"),
        (DatapointsLoader, DATA_FOLDER / "timeseries_datapoints"),
    ],
)
def test_loader_class(
    loader_cls: type[ResourceLoader], directory: Path, cognite_client_approval: ApprovalCogniteClient, data_regression
):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client
    cdf_tool.data_set_id = 999

    loader_cls.create_loader(cdf_tool).deploy_resources(directory, cdf_tool, drop=False, dry_run=False)

    dump = cognite_client_approval.dump()
    data_regression.check(dump, fullpath=SNAPSHOTS_DIR / f"{directory.name}.yaml")


def test_upsert_data_set(cognite_client_approval: ApprovalCogniteClient):
    cdf_tool = MagicMock(spec=CDFToolConfig)
    cdf_tool.verify_client.return_value = cognite_client_approval.mock_client
    cdf_tool.verify_capabilities.return_value = cognite_client_approval.mock_client

    loader = DataSetsLoader.create_loader(cdf_tool)
    loaded = loader.load_resource(DATA_FOLDER / "data_sets" / "1.my_datasets.yaml", cdf_tool, skip_validation=False)
    assert len(loaded) == 2

    first = DataSet.load(loaded[0].dump())
    # Set the properties that are set on the server side
    first.id = 42
    first.created_time = 42
    first.last_updated_time = 42
    # Simulate that the data set is already in CDF
    cognite_client_approval.append(DataSet, first)

    to_create, to_change, unchanged = loader._to_create_changed_unchanged_triple(loaded)

    assert len(to_create) == 1
    assert len(to_change) == 0
    assert len(unchanged) == 1


class TestAuthLoader:
    def test_load_id_scoped_dataset_acl(self, cdf_tool_config: CDFToolConfig, monkeypatch: MonkeyPatch):
        loader = AuthLoader.create_loader(cdf_tool_config, "all")

        file_content = """
name: 'some_name'
sourceId: '123'
capabilities:
  - datasetsAcl:
      actions:
        - READ
        - OWNER
      scope:
        idScope: { ids: ["site:001:b60:ds"] }
"""

        mock_read_yaml_file({"group_file.yaml": yaml.safe_load(file_content)}, monkeypatch)

        loaded = loader.load_resource(Path("group_file.yaml"), cdf_tool_config, skip_validation=True)

        assert loaded.name == "some_name"
