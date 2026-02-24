from collections.abc import Iterator

import pytest
import responses

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.charts import Chart, ChartList, ChartWrite
from cognite_toolkit._cdf_tk.client.resource_classes.charts_data import (
    ChartCoreTimeseries,
    ChartData,
    ChartScheduledCalculation,
    ChartSettings,
    ChartSource,
    ChartThreshold,
    ChartTimeseries,
    ChartWorkflow,
    UserInfo,
)

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


def chart_data_generator() -> Iterator[tuple]:
    yield pytest.param(
        [
            {
                "type": "timeseries",
                "id": "test-ts-id",
                "name": "name",
                "color": "#6929c4",
                "tsId": 123,
                "tsExternalId": "externalID",
                "lineWeight": 1,
                "lineStyle": "solid",
                "interpolation": "linear",
                "displayMode": "lines",
                "enabled": False,
                "unit": "",
                "originalUnit": "",
                "preferredUnit": "",
                "description": "description",
                "range": [0.0, 50.0],
                "createdAt": 1717487277745,
            },
            {
                "color": "#005d5d",
                "createdAt": 1698276540866,
                "description": "備ヒカル",
                "displayMode": "lines",
                "enabled": False,
                "id": "e7ebd31a-ae98-4db6-9cca-442cf3e6f703",
                "interpolation": "linear",
                "lineStyle": "solid",
                "lineWeight": 1.0,
                "name": "3421",
                "originalUnit": "Nm3/m",
                "preferredUnit": "Nm3/m",
                "range": [],
                "tsExternalId": "my-id-2805664976192084",
                "tsId": 2805664976192084,
                "type": "timeseries",
                "unit": "Nm3/m",
            },
        ],
        ChartTimeseries,
        id=ChartTimeseries.__name__,
    )
    yield pytest.param(
        [{"type": "timeseries", "id": "test-ts-id"}],
        ChartSource,
        id=ChartSource.__name__,
    )
    yield pytest.param(
        [
            {
                "id": "test-threshold-id",
                "name": "name",
                "visible": True,
                "sourceId": "test-ts-id",
                "upperLimit": 0,
                "type": "under",
                "filter": {"minUnit": "seconds", "maxUnit": "hours"},
                "calls": [
                    {
                        "hash": 1238826452,
                        "callId": "test-call-id",
                        "callDate": 1717487580156,
                        "id": "test-call-id",
                        "status": "Pending",
                    }
                ],
            }
        ],
        ChartThreshold,
        id=ChartThreshold.__name__,
    )
    yield pytest.param(
        [
            {
                "version": "v2",
                "type": "workflow",
                "id": "test-workflow-id",
                "name": "name",
                "color": "#005d5d",
                "enabled": True,
                "settings": {"autoAlign": True},
                "lineWeight": 1.0,
                "lineStyle": "solid",
                "interpolation": "linear",
                "unit": "",
                "preferredUnit": "",
                "range": [0.0, 50.0],
                "createdAt": 1717487389841,
                "flow": {
                    "zoom": 1.032008279029462,
                    "elements": [
                        {
                            "id": "test-output-id",
                            "type": "CalculationOutput",
                            "position": {"x": 754.0, "y": 87.0},
                            "data": {},
                            "source": None,
                            "target": None,
                            "sourceHandle": None,
                            "targetHandle": None,
                        },
                        {
                            "id": "test-input-id",
                            "data": {"type": "timeseries", "selectedSourceId": "test-ts-id"},
                            "type": "CalculationInput",
                            "position": {"x": 37.0, "y": 191.0},
                            "source": None,
                            "target": None,
                            "sourceHandle": None,
                            "targetHandle": None,
                        },
                        {
                            "id": "test-edge-id",
                            "source": "test-output-id",
                            "target": "test-input-id",
                            "sourceHandle": "out-result-0",
                            "targetHandle": "datapoints",
                            "data": {},
                            "type": None,
                            "position": None,
                        },
                    ],
                    "position": [-36.883849748699845, -20.958125092217585],
                },
                "calls": [
                    {
                        "id": "test-call-id",
                        "hash": 1038003377,
                        "callId": "test-call-id",
                        "status": "Pending",
                        "callDate": 1755525052454,
                    }
                ],
            }
        ],
        ChartWorkflow,
        id=ChartWorkflow.__name__,
    )
    yield pytest.param(
        [
            {
                "version": "v2",
                "type": "scheduledCalculation",
                "id": "test-scheduled-calculation-id",
                "name": "name",
                "color": "#1192e8",
                "enabled": True,
                "settings": {"autoAlign": True},
                "description": "vfgf",
                "lineWeight": 1.0,
                "lineStyle": "solid",
                "interpolation": "linear",
                "unit": "psi",
                "preferredUnit": "psi",
                "range": [0.0, 50.0],
                "createdAt": 1755525754414,
                "flow": {
                    "zoom": 1.0,
                    "elements": [
                        {
                            "id": "test-output-id",
                            "type": "CalculationOutput",
                            "position": {"x": 400.0, "y": 150.0},
                            "data": {},
                            "source": None,
                            "target": None,
                            "sourceHandle": None,
                            "targetHandle": None,
                        }
                    ],
                    "position": [0.0, 0.0],
                },
            }
        ],
        ChartScheduledCalculation,
        id=ChartScheduledCalculation.__name__,
    )
    yield pytest.param(
        [
            {
                "type": "coreTimeseries",
                "id": "9910137c-227d-4951-a9bf-284c04c48e51",
                "color": "#6929c4",
                "nodeReference": {
                    "space": "charts",
                    "externalId": "test-scheduled-calculation-fdm-time-series_1739436335052_TS",
                },
                "viewReference": {"space": "cdf_cdm", "externalId": "CogniteTimeSeries", "version": "v1"},
                "name": "test-scheduled-calculation-fdm-time-series",
                "lineWeight": 1,
                "lineStyle": "solid",
                "interpolation": "hv",
                "displayMode": "lines",
                "enabled": True,
                "preferredUnit": "",
                "range": [None, None],
                "createdAt": 1743764782866,
            }
        ],
        ChartCoreTimeseries,
        id=ChartCoreTimeseries.__name__,
    )


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
        dumped = loaded.model_dump(mode="json", by_alias=True, exclude_unset=True)

        assert dumped == chart_data, f"Expected {chart_data}, but got {dumped}"

    @pytest.mark.parametrize("chart_data_dict, expected_cls", list(chart_data_generator()))
    def test_serialize_deserialize_chart_data_components(
        self, chart_data_dict: list[dict], expected_cls: type[BaseModelObject]
    ) -> None:
        """Test that ChartData components can be serialized and deserialized correctly."""
        # We validate with extra="ignore" to ensure that we are including all fields that are in the test data.
        loaded_items = [expected_cls.model_validate(item, extra="ignore") for item in chart_data_dict]
        dumped_items = [item.dump(camel_case=True) for item in loaded_items]

        assert dumped_items == chart_data_dict, f"Expected {chart_data_dict}, but got {dumped_items}"
