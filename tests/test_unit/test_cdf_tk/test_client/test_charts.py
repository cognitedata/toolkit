import pytest
import responses

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartList, ChartWrite


class ToolkitConfig:
    pass


class TestChartAPI:
    CHART = Chart(
        external_id="chart",
        created_time=1,
        last_updated_time=2,
        visibility="PUBLIC",
        data={
            "version": 1,
            "name": "TestNew",
            "dateFrom": "2025-04-26T22:00:00.000Z",
            "dateTo": "2025-05-27T21:59:59.999Z",
            "userInfo": {"id": "toolkit_test_user", "email": "support@cognite.com", "displayName": "Toolkit Test User"},
            "timeSeriesCollection": [],
            "workflowCollection": [],
            "sourceCollection": [],
            "thresholdCollection": [],
            "scheduledCalculationCollection": [],
            "settings": {"showYAxis": True, "showMinMax": True, "showGridlines": True, "mergeUnits": True},
        },
        owner_id="toolkit_test_user",
    )

    @pytest.mark.parametrize(
        "items, expected_return_cls",
        [
            pytest.param(
                CHART.as_write(),
                Chart,
                id="Single ChartWrite",
            ),
            pytest.param(
                [CHART.as_write()],
                ChartList,
                id="List of ChartWrite",
            ),
        ],
    )
    def test_upsert_return_type(
        self,
        items: ChartWrite | list[ChartWrite],
        expected_return_cls: type,
        toolkit_config: ToolkitClientConfig,
    ) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = f"{toolkit_config.base_url}/apps/v1/projects/{toolkit_config.project}/storage/charts/charts"

        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.PUT,
                url,
                status=200,
                json={"items": [self.CHART.dump()]},
            )
            result = client.charts.upsert(items)

        assert isinstance(result, expected_return_cls)

    @pytest.mark.parametrize(
        "external_id, expected_return_cls",
        [
            pytest.param(
                CHART.external_id,
                Chart,
                id="Single Chart",
            ),
            pytest.param(
                [CHART.external_id],
                ChartList,
                id="List of Chart",
            ),
        ],
    )
    def test_retrieve_return_type(
        self, external_id: str | list[str], expected_return_cls: type, toolkit_config: ToolkitClientConfig
    ) -> None:
        client = ToolkitClient(config=toolkit_config, enable_set_pending_ids=True)
        url = f"{toolkit_config.base_url}/apps/v1/projects/{toolkit_config.project}/storage/charts/charts/byids"
        with responses.RequestsMock() as rsps:
            rsps.add(
                responses.POST,
                url,
                status=200,
                json={"items": [self.CHART.dump()]},
            )
            result = client.charts.retrieve(external_id=external_id)

        assert isinstance(result, expected_return_cls)
