import pytest
import responses

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartList, ChartWrite
from cognite_toolkit._cdf_tk.client.data_classes.charts_data import ChartData, ChartSettings, UserInfo
from tests.test_unit.utils import FakeCogniteResourceGenerator

CHART = Chart(
    external_id="chart",
    created_time=1,
    last_updated_time=2,
    visibility="PUBLIC",
    data=ChartData(
        version=1,
        name="TestNew",
        date_from="2025-04-26T22:00:00.000Z",
        date_to="2025-05-27T21:59:59.999Z",
        user_info=UserInfo(id="toolkit_test_user", email="support@cognite.com", display_name="Toolkit Test User"),
        settings=ChartSettings(
            show_y_axis=True,
            show_min_max=True,
            show_gridlines=True,
            merge_units=True,
        ),
    ),
    owner_id="toolkit_test_user",
)


class TestChartAPI:
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
                json={"items": [CHART.dump()]},
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
                json={"items": [CHART.dump()]},
            )
            result = client.charts.retrieve(external_id=external_id)

        assert isinstance(result, expected_return_cls)


class TestChartDTOs:
    def test_chart_data_changed(self) -> None:
        """The ChartData is frontend of the Chart object, and it is not enforced in any way by the backend API.
        Thus, it can change completely without any notice. This tests ensures that whatever the ChartData is,
        the serialization and deserialization works correctly.
        """
        chart_data = {
            "this": "is",
            "completely": {
                "changed": ["compared", "to", "the", "previous", "version"],
            },
            "and": 123,
            "itAlso": {"hasSomeNumbers": [1, 2, 3, 4, 5]},
        }
        loaded = ChartData._load(chart_data)
        dumped = loaded.dump(camel_case=False)

        assert dumped == chart_data, f"Expected {chart_data}, but got {dumped}"

    def test_serialize_deserialize_chart(self) -> None:
        """Test that Chart can be serialized and deserialized correctly."""
        instance = FakeCogniteResourceGenerator(seed=42).create_instance(ChartData)
        instance._unknown_fields = {"extraField": "extraValue"}

        dumped = instance.dump(camel_case=True)

        loaded = ChartData._load(dumped)

        assert loaded.dump() == instance.dump(), f"Expected {instance}, but got {loaded}"
