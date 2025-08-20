from contextlib import suppress
from uuid import uuid4

import pytest
from cognite.client.data_classes import TimeSeries, TimeSeriesWrite, UserProfile
from cognite.client.exceptions import CogniteNotFoundError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.charts import Chart, ChartWrite
from cognite_toolkit._cdf_tk.client.data_classes.charts_data import (
    ChartData,
    ChartSource,
    ChartTimeseries,
    UserInfo,
)


@pytest.fixture(scope="session")
def timeseries(toolkit_client: ToolkitClient) -> TimeSeries:
    """Fixture to create a test TimeSeries."""
    ts = TimeSeriesWrite(
        external_id="toolkit_test_chart_ts",
        name="Toolkit Test Chart Time Series",
        is_step=False,
        is_string=False,
    )
    res = toolkit_client.time_series.retrieve(external_id=ts.external_id)
    if res:
        return res
    return toolkit_client.time_series.create(ts)


class TestChartsAPI:
    def test_upsert_retrieve_list_delete(self, toolkit_client: ToolkitClient, timeseries: TimeSeries) -> None:
        me = toolkit_client.iam.user_profiles.me()
        chart_id = str(uuid4())
        chart = self.create_chart(chart_id, me, timeseries)

        created: Chart | None = None
        try:
            created = toolkit_client.charts.upsert(chart)

            retrieved = toolkit_client.charts.retrieve(external_id=chart_id)
            assert retrieved.external_id == chart_id

            listed = toolkit_client.charts.list(is_owned=True, visibility="PUBLIC")
            assert any(c.external_id == chart_id for c in listed)

            toolkit_client.charts.delete(external_id=chart_id)

            retrieved2 = toolkit_client.charts.retrieve(external_id=chart_id)
            assert retrieved2 is None, "Chart should be deleted and not retrievable."
        finally:
            if created:
                with suppress(CogniteNotFoundError):
                    toolkit_client.charts.delete(chart_id)

    @staticmethod
    def create_chart(chart_id: str, me: UserProfile, ts: TimeSeries) -> ChartWrite:
        ts_chart_id = str(uuid4())
        return ChartWrite(
            external_id=chart_id,
            visibility="PUBLIC",
            data=ChartData(
                version=1,
                name="Toolkit Test Chart",
                date_from="1916-06-15T12:35:46.880Z",
                date_to="2279-04-14T07:10:41.670Z",
                user_info=UserInfo(id=me.user_identifier, email=me.email, display_name=me.display_name),
                live_mode=False,
                time_series_collection=[
                    ChartTimeseries(
                        type="timeseries",
                        id=ts_chart_id,
                        name=ts.name,
                        color="#1192e8",
                        ts_id=ts.id,
                        ts_external_id=ts.external_id,
                        line_weight=1.0,
                        line_style="solid",
                        interpolation="linear",
                        display_mode="lines",
                        enabled=True,
                        unit="",
                        original_unit="",
                        preferred_unit="",
                        description="-",
                        range=(None, None),
                        created_at=1755456114956,
                    )
                ],
                workflow_collection=[],
                source_collection=[ChartSource(type="timeseries", id=ts_chart_id)],
            ),
        )
