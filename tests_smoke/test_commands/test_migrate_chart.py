import re
from collections.abc import Iterable
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from pytest_regressions.data_regression import DataRegressionFixture

from cognite_toolkit._cdf_tk.apps import MigrateApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import InternalId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.chart import ChartRequest, ChartResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceSource,
    NodeRequest,
    SpaceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from cognite_toolkit._cdf_tk.commands._migrate.data_model import INSTANCE_SOURCE_VIEW_ID
from cognite_toolkit._cdf_tk.dataio import ChartIO
from cognite_toolkit._cdf_tk.dataio.selectors import ChartExternalIdSelector
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonReader

THIS_DIR = Path(__file__).parent
TEST_DATA = THIS_DIR / "chart_test_data.yaml"

UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)


@pytest.fixture
def load_toolkit_client(toolkit_client: ToolkitClient) -> None:
    with patch(f"{MigrateApp.__module__}.EnvironmentVariables") as mock_env:
        mock_env.create_from_environment.return_value.get_client.return_value = toolkit_client


@pytest.fixture()
def legacy_chart(
    toolkit_client: ToolkitClient, smoke_space: SpaceResponse, smoke_dataset: DataSetResponse
) -> Iterable[ChartResponse]:
    """Create a legacy chart with a classic timeseries and the InstanceSource mapping node."""
    client = toolkit_client

    raw = TEST_DATA.read_text()
    chart_data = yaml.safe_load(raw)
    chart = ChartRequest._load(chart_data)
    ts_collection = chart.data.time_series_collection
    if not (ts_collection and isinstance(ts_external_id := ts_collection[0].ts_external_id, str)):
        raise AssertionError(
            "Chart migration failed - test data is invalid. Expected a single time series with a string external ID."
        )
    ts = create_migrate_timeseries(client, ts_external_id, smoke_space.space, smoke_dataset.id)

    ts_collection[0].ts_id = ts.id
    if chart.data.user_info:
        chart.data.user_info.id = client.user_profiles.me().user_identifier

    if not chart.scheduled_calculations:
        raise AssertionError("Chart migration failed - no scheduled calculations.")

    calculation = chart.scheduled_calculations[0]
    if calculation.target_timeseries_external_id is None:
        raise AssertionError(
            "Chart migration failed - scheduled calculation does not have a target timeseries external ID."
        )
    _ = create_migrate_timeseries(
        client, calculation.target_timeseries_external_id, smoke_space.space, smoke_dataset.id
    )

    if not chart.monitoring_jobs:
        raise AssertionError("Chart migration failed - no monitoring jobs.")
    monitoring_job = chart.monitoring_jobs[0]

    calculation.nonce = client.iam.sessions.create().nonce
    monitoring_job.nonce = client.iam.sessions.create().nonce
    alert_cannels = client.alerts.channels.list()
    if len(alert_cannels) == 0:
        raise AssertionError("Chart migration failed - no alert cannels available.")
    monitoring_job.channel_id = alert_cannels[0].id

    if client.charts.scheduled_calculations.retrieve([calculation.as_id()], ignore_unknown_ids=True):
        client.charts.scheduled_calculations.delete([calculation.as_id()])
    _ = client.charts.scheduled_calculations.create([calculation])

    if client.charts.monitoring_jobs.retrieve([monitoring_job.as_id()], ignore_unknown_ids=True):
        client.charts.monitoring_jobs.delete([monitoring_job.as_id()])

    created_job = client.charts.monitoring_jobs.create([monitoring_job])[0]
    # Update internal ID used to link monitoring job with Chart UI element.
    chart.monitoring_jobs[0].id = created_job.id
    chart.data.monitoring_jobs[0].id = created_job.id  # type: ignore[index]

    created_charts = client.charts.create([chart])

    yield created_charts[0]

    client.charts.scheduled_calculations.delete([calculation.as_id()])
    client.charts.monitoring_jobs.delete([monitoring_job.as_id()])
    client.charts.delete([chart.as_id()])


class TestMigrateChart:
    @pytest.mark.usefixtures("load_toolkit_client")
    def test_migrate_data(
        self,
        legacy_chart: ChartResponse,
        toolkit_client: ToolkitClient,
        tmp_path: Path,
        data_regression: DataRegressionFixture,
    ) -> None:
        if len(legacy_chart.data.time_series_collection or []) == 0:
            raise AssertionError("Chart migration failed. Legacy chart does not have any time series.")
        if len(legacy_chart.data.core_timeseries_collection or []) != 0:
            raise AssertionError(
                "Chart migration failed. Legacy chart has core timeseries, expected only classic timeseries."
            )

        MigrateApp.charts(
            ctx=MagicMock(),
            external_id=[legacy_chart.external_id],
            log_dir=tmp_path,
            dry_run=False,
            verbose=True,
        )
        log_files = [file for file in tmp_path.rglob("*.ndjson") if file.is_file()]
        if log_files:
            print(f"Migration log files found in {tmp_path}: {[file.name for file in log_files]}")
            for log_file in log_files:
                for chunk in NDJsonReader(log_file).read_chunks():
                    print(chunk)
        else:
            print("No migration log files found. This means there were no issues.")

        # Use ChartsIO to include backend services.
        page = next(
            iter(
                ChartIO(toolkit_client, skip_backend_services=False).stream_data(
                    selector=ChartExternalIdSelector(external_ids=(legacy_chart.external_id,))
                )
            )
        )
        if len(page.items) != 1:
            raise AssertionError("Charts migration failed. Failed to retrieve migrated chart.")
        migrated_chart = page.items[0].item
        if migrated_chart.data.time_series_collection:
            classic_ts = [ts.ts_external_id for ts in migrated_chart.data.time_series_collection]
            raise AssertionError(
                f"Charts migration failed. {humanize_collection(classic_ts)} classical time series is still present"
            )
        migrated_refs = len(migrated_chart.data.core_timeseries_collection or [])
        classic_refs = len(legacy_chart.data.time_series_collection or [])
        if migrated_refs != classic_refs:
            raise AssertionError(
                f"Chart migration failed. Expected {classic_refs} migrated timeseries in migrated chart, found {migrated_refs}"
            )

        request = migrated_chart.as_request_resource()
        for job in request.monitoring_jobs or []:
            # Replace internal ID with instance ID if possible.
            # Ensure a more consistent snapshot.
            if job.model.timeseries_id and not job.model.timeseries_instance_id:
                lookup = toolkit_client.tool.timeseries.retrieve([InternalId(id=job.model.timeseries_id)])[0]
                if lookup.instance_id:
                    job.model.timeseries_instance_id = lookup.instance_id

            if job.model.timeseries_id and job.model.timeseries_instance_id:
                # Remove internal ID if we have an instance ID.
                job.model.timeseries_id = None

        dumped = request.dump()
        dumped["monitoringJobs"] = [job.dump() for job in request.monitoring_jobs or []] or None
        dumped["scheduledCalculations"] = [
            calculation.dump() for calculation in request.scheduled_calculations or []
        ] or None
        # Changes depending on test run and service principal used.
        del dumped["data"]["userInfo"]["id"]
        del dumped["data"]["monitoringJobs"][0]["id"]
        del dumped["monitoringJobs"][0]["channelId"]

        data_regression.check({"chart": dumped})


def create_migrate_timeseries(
    client: ToolkitClient,
    external_id: str,
    space: str,
    data_set_id: int,
) -> TimeSeriesResponse:
    """Creating a classic timeseries and migrate it.

    This is assumed to be persistent, so it will only create it the first time.
    """
    timeseries = TimeSeriesRequest(
        external_id=external_id,
        is_string=False,
        is_step=False,
        data_set_id=data_set_id,
    )
    existing = client.tool.timeseries.retrieve([timeseries.as_id()], ignore_unknown_ids=True)
    if existing and existing[0].instance_id is not None:
        return existing[0]
    elif existing:
        retrieved = existing[0]
    else:
        retrieved = client.tool.timeseries.create([timeseries])[0]
    node_id = NodeId(space=space, external_id=external_id)
    if retrieved.pending_instance_id is None:
        retrieved = client.tool.timeseries.set_pending_ids(
            [PendingInstanceId(pending_instance_id=node_id, external_id=external_id)]
        )[0]

    timeseries_node = NodeRequest(
        space=node_id.space,
        external_id=node_id.external_id,
        sources=[
            InstanceSource(
                source=ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1"),
                properties={
                    "type": "numeric",
                    "isStep": False,
                },
            ),
            # Lineage of migration, used in Chart migration.
            InstanceSource(
                source=INSTANCE_SOURCE_VIEW_ID,
                properties={
                    "resourceType": "timeseries",
                    "dataSetId": data_set_id,
                    "id": retrieved.id,
                    "classicExternalId": external_id,
                },
            ),
        ],
    )
    client.tool.instances.create([timeseries_node])
    return retrieved
