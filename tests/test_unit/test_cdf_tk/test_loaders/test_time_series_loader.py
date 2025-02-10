import yaml

from cognite_toolkit._cdf_tk.loaders import TimeSeriesLoader
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.approval_client.client import LookUpAPIMock


class TestTimeSeriesLoader:
    timeseries_yaml = """
externalId: pi_160696
name: VAL_23-PT-92504:X.Value
dataSetExternalId: ds_timeseries_oid
isString: false
metadata:
  compdev: '0'
  location5: '2'
isStep: false
description: PH 1stStgSuctCool Gas Out
"""

    def test_load_skip_validation_with_preexisting_dataset(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        loader = TimeSeriesLoader(toolkit_client_approval.mock_client, None)
        ts_dict = yaml.safe_load(self.timeseries_yaml)
        data_set_external_id = ts_dict["dataSetExternalId"]
        expected_id = LookUpAPIMock._create_id(data_set_external_id)

        loaded = loader.load_resource(ts_dict, is_dry_run=False)

        assert loaded.data_set_id == expected_id

    def test_load_skip_validation_no_preexisting_dataset(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        loader = TimeSeriesLoader(toolkit_client_approval.mock_client, None)
        ts_dict = yaml.safe_load(self.timeseries_yaml)

        def id_missing(*args):
            return -1

        toolkit_client_approval.mock_client.lookup.data_sets.id.side_effect = id_missing

        loaded = loader.load_resource(ts_dict, is_dry_run=False)

        assert loaded.data_set_id == -1
