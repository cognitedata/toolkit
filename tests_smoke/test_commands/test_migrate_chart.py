import re
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from pytest_regressions.data_regression import DataRegressionFixture

from cognite_toolkit._cdf_tk.apps import MigrateApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import RequestMessage
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NodeId, ViewId
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
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonReader

THIS_DIR = Path(__file__).parent
TEST_DATA = THIS_DIR / "chart_test_data.yaml"

UUID_PATTERN = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)


@pytest.fixture
def load_toolkit_client(toolkit_client: ToolkitClient) -> None:
    with patch(f"{MigrateApp.__module__}.EnvironmentVariables") as mock_env:
        mock_env.create_from_environment.return_value.get_client.return_value = toolkit_client


def _replace_uuids(obj: object, uuid_map: dict[str, str]) -> object:
    """Recursively replace UUIDs in a nested dict/list with stable placeholders."""
    if isinstance(obj, str) and UUID_PATTERN.fullmatch(obj):
        if obj not in uuid_map:
            uuid_map[obj] = f"UUID_{len(uuid_map)}"
        return uuid_map[obj]
    if isinstance(obj, dict):
        return {k: _replace_uuids(v, uuid_map) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_replace_uuids(item, uuid_map) for item in obj]
    return obj


def _restore_chart(client: ToolkitClient, chart_request: ChartRequest) -> None:
    """Restore a chart to its original state via the per-chart PUT endpoint."""
    url = client.config.create_app_url(f"/storage/charts/charts/{chart_request.external_id}")
    dumped = chart_request.dump()
    dumped.pop("externalId", None)
    client.http_client.request_single_retries(
        RequestMessage(
            endpoint_url=url,
            method="PUT",
            body_content=dumped,
        )
    )


@pytest.fixture()
def legacy_chart(
    toolkit_client: ToolkitClient, smoke_space: SpaceResponse, smoke_dataset: DataSetResponse
) -> ChartResponse:
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

    # The Chart Create endpoint is an upsert. So this will restore the chart
    # for each run.
    created_charts = client.charts.create([chart])

    return created_charts[0]


class TestMigrateChart:
    def test_migrate_data(
        self,
        legacy_chart: ChartResponse,
        toolkit_client: ToolkitClient,
        tmp_path: Path,
        data_regression: DataRegressionFixture,
    ) -> None:
        if len(legacy_chart.data.time_series_collection or []) == 0:
            raise AssertionError("Chart migration failed. Legacy chart does not have no time series.")
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

        migrated_charts = toolkit_client.charts.retrieve([ExternalId(external_id=legacy_chart.external_id)])
        if len(migrated_charts) != 1:
            raise AssertionError("Charts migration failed. Failed to retrieve migrated chart.")
        migrated_chart = migrated_charts[0]
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

        dumped = migrated_chart.as_request_resource().dump()
        uuid_map: dict[str, str] = {}
        stabilised = _replace_uuids(dumped, uuid_map)
        data_regression.check({"chart": stabilised})


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
