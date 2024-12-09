from pathlib import Path

import yaml
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.loaders import TimeSeriesLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig
from tests.test_unit.approval_client import ApprovalToolkitClient
from tests.test_unit.utils import mock_read_yaml_file


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

    def test_load_skip_validation_no_preexisting_dataset(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        loader = TimeSeriesLoader(toolkit_client_approval.mock_client, None)
        mock_read_yaml_file({"timeseries.yaml": yaml.safe_load(self.timeseries_yaml)}, monkeypatch)
        loaded = loader.load_resource_file(Path("timeseries.yaml"), cdf_tool_real, skip_validation=True)

        assert len(loaded) == 1
        assert loaded[0].data_set_id == -1

    def test_load_skip_validation_with_preexisting_dataset(
        self,
        toolkit_client_approval: ApprovalToolkitClient,
        cdf_tool_real: CDFToolConfig,
        monkeypatch: MonkeyPatch,
    ) -> None:
        cdf_tool_real._cache.data_set_id_by_external_id["ds_timeseries_oid"] = 12345
        loader = TimeSeriesLoader(toolkit_client_approval.mock_client, None)

        mock_read_yaml_file({"timeseries.yaml": yaml.safe_load(self.timeseries_yaml)}, monkeypatch)

        loaded = loader.load_resource_file(Path("timeseries.yaml"), cdf_tool_real, skip_validation=True)

        assert len(loaded) == 1
        assert loaded[0].data_set_id == 12345
