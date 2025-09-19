from collections.abc import Sequence

import pytest
import responses
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeriesList


class TestExtendedTimeSeriesAPI:
    @pytest.mark.parametrize(
        "id, external_id, expected_list",
        [
            pytest.param(None, "my_timeseries", False, id="External ID only"),
            pytest.param(123, None, False, id="ID only"),
            pytest.param([123, 456], None, True, id="List of IDs"),
            pytest.param(None, ["my_timeseries", "my_other_timeseries"], True, id="List of External IDs"),
            pytest.param([123, 456], ["my_timeseries", "my_other_timeseries"], True, id="List of IDs and External IDs"),
        ],
    )
    def test_unlink_instance_ids_valid(
        self,
        id: int | Sequence[int] | None,
        external_id: str | SequenceNotStr[str] | None,
        expected_list: bool,
        toolkit_config: ToolkitClientConfig,
    ) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = f"{toolkit_config.base_url}/api/v1/projects/test-project/timeseries/unlink-instance-ids"
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [{"externalId": "does-not-matter", "id": 123, "createdTime": 0, "lastUpdatedTime": 0}]},
            )
            result = client.time_series.unlink_instance_ids(id=id, external_id=external_id)

        is_list = isinstance(result, ExtendedTimeSeriesList)
        assert is_list == expected_list, f"Expected result to be a list: {expected_list}, got {is_list}"

    def test_unlink_instance_ids_none_return_none(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        result = client.time_series.unlink_instance_ids(id=None, external_id=None)
        assert result is None

    def test_unlink_instance_ids_invalid(self, toolkit_config: ToolkitClientConfig) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        with pytest.raises(
            ValueError, match=r"Cannot specify both id and external_id as single values. Use one or the other."
        ):
            client.time_series.unlink_instance_ids(id=123, external_id="123")
