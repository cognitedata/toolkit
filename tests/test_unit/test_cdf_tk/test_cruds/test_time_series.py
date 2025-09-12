import yaml
from cognite.client.data_classes import TimeSeriesWrite
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.cruds import TimeSeriesCRUD
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
        loader = TimeSeriesCRUD(toolkit_client_approval.mock_client, None)
        ts_dict = yaml.safe_load(self.timeseries_yaml)
        data_set_external_id = ts_dict["dataSetExternalId"]
        expected_id = LookUpAPIMock.create_id(data_set_external_id)

        loaded = loader.load_resource(ts_dict, is_dry_run=False)

        assert loaded.data_set_id == expected_id

    def test_load_skip_validation_no_preexisting_dataset(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
    ) -> None:
        loader = TimeSeriesCRUD(toolkit_client_approval.mock_client, None)
        ts_dict = yaml.safe_load(self.timeseries_yaml)

        def id_missing(*args):
            return -1

        toolkit_client_approval.mock_client.lookup.data_sets.id.side_effect = id_missing

        loaded = loader.load_resource(ts_dict, is_dry_run=False)

        assert loaded.data_set_id == -1

    def test_load_timeseries_ref_not_yet_deployed(self) -> None:
        actual_is_dry_run: list[bool] = []

        def mock_id_lookup(
            external_id: str | SequenceNotStr[str], is_dry_run: bool = False, allow_empty: bool = False
        ) -> int | list[int]:
            actual_is_dry_run.append(is_dry_run)
            return -1 if isinstance(external_id, str) else [-1] * len(external_id)

        with monkeypatch_toolkit_client() as client:
            client.lookup.data_sets.id.side_effect = mock_id_lookup
            client.lookup.assets.id.side_effect = mock_id_lookup
            client.lookup.security_categories.id.side_effect = mock_id_lookup
            loader = TimeSeriesCRUD.create_loader(client)

        resource = {
            "externalId": "MyTimeseries",
            "assetExternalId": "my_asset",
            "dataSetExternalId": "my_dataset",
            "securityCategoryNames": ["my_security_category"],
        }

        ts = loader.load_resource(resource.copy(), is_dry_run=True)
        _ = loader.load_resource(resource.copy(), is_dry_run=False)

        assert isinstance(ts, TimeSeriesWrite)
        assert ts.asset_id == -1
        assert ts.data_set_id == -1
        assert ts.security_categories == [-1]
        assert actual_is_dry_run == [True] * 3 + [False] * 3
