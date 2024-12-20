from unittest.mock import MagicMock

from cognite.client.data_classes import DataSet

from cognite_toolkit._cdf_tk.commands import DeployCommand
from cognite_toolkit._cdf_tk.loaders import DataSetsLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.data import LOAD_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestDataSetsLoader:
    def test_upsert_data_set(self, toolkit_client_approval: ApprovalToolkitClient):
        cdf_tool = MagicMock(spec=CDFToolConfig)
        cdf_tool.verify_authorization.return_value = toolkit_client_approval.mock_client
        cdf_tool.client = toolkit_client_approval.mock_client
        cdf_tool.toolkit_client = toolkit_client_approval.mock_client

        loader = DataSetsLoader.create_loader(cdf_tool, None)
        loaded = loader.load_resource_file(LOAD_DATA / "data_sets" / "1.my_datasets.yaml", cdf_tool, is_dry_run=False)
        assert len(loaded) == 2

        first = DataSet.load(loaded[0].dump())
        # Set the properties that are set on the server side
        first.id = 42
        first.created_time = 42
        first.last_updated_time = 42
        # Simulate that the data set is already in CDF
        toolkit_client_approval.append(DataSet, first)
        cmd = DeployCommand(print_warning=False)
        to_create, to_change, unchanged = cmd.to_create_changed_unchanged_triple(loaded, loader)

        assert len(to_create) == 1
        assert len(to_change) == 0
        assert len(unchanged) == 1
