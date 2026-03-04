from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes import FileMetadataUpdate, TimeSeriesUpdate
from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.apps._migrate_app import MigrateApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceRequest,
    InstanceSource,
    NodeRequest,
    SpaceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest

THIS_DIR = Path(__file__).parent
TEST_DATA = THIS_DIR / "infield_test_data.yaml"


@pytest.fixture
def load_toolkit_client(toolkit_client: ToolkitClient) -> None:
    with patch(f"{MigrateApp.__module__}.EnvironmentVariables") as mock_env:
        mock_env.create_from_environment.return_value.get_client.return_value = toolkit_client


@pytest.fixture()
def set_up_infield_legacy(toolkit_client: ToolkitClient, smoke_space: SpaceResponse) -> None:
    client = toolkit_client
    source_space = smoke_space.space
    myself = client.user_profiles.me()
    instances, timeseries, files = load_infield_source_data(user_id=myself.user_identifier, source_space=source_space)

    for ts in timeseries:
        instances.append(
            NodeRequest(
                space=source_space,
                external_id=cast(str, ts.external_id),
                sources=[
                    InstanceSource(
                        source=ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1"),
                        properties={
                            "isStep": False,
                            "type": "numeric",
                        },
                    )
                ],
            )
        )

    for file in files:
        instances.append(
            NodeRequest(
                space=source_space,
                external_id=cast(str, file.external_id),
                sources=[
                    InstanceSource(
                        source=ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1"),
                        properties={
                            "name": file.name,
                            "mimeType": "text/plain",
                        },
                    )
                ],
            )
        )

    _ = client.tool.instances.create(instances)

    timeseries_updates: list[TimeSeriesUpdate] = []
    for ts in timeseries:
        external_id = cast(str, ts.external_id)
        ts_update = TimeSeriesUpdate(
            instance_id=dm.NodeId(space=source_space, external_id=external_id),
        ).external_id.set(external_id)
        timeseries_updates.append(ts_update)
    file_updates: list[FileMetadataUpdate] = []
    for file in files:
        external_id = cast(str, file.external_id)
        file_update = FileMetadataUpdate(
            instance_id=dm.NodeId(space=source_space, external_id=external_id),
        ).external_id.set(external_id)
        file_updates.append(file_update)
    _ = client.time_series.update(timeseries_updates)
    _ = client.files.update(file_updates)


class TestMigrateInfield:
    @pytest.mark.usefixtures("load_toolkit_client")
    def test_migrate_data(
        self, toolkit_client: ToolkitClient, source_space: SpaceResponse, target_space: SpaceResponse, tmp_path: Path
    ) -> None:
        MigrateApp.infield_data(
            ctx=MagicMock(),
            source_space=source_space.space,
            target_space=target_space.space,
            log_dir=tmp_path,
            dry_run=False,
            verbose=True,
        )

        # Here you would add assertions to verify the expected behavior of the migration
        # For example, you could check that the correct API calls were made to the toolkit client
        # or that the expected data was migrated from the source space to the target space.


def load_infield_source_data(
    user_id: str, source_space: str
) -> tuple[list[InstanceRequest], list[TimeSeriesRequest], list[FileMetadataRequest]]:
    raw_data = TEST_DATA.read_text().replace("{{user_id}}", user_id).replace("{{source_space}}", source_space)
    instances = TypeAdapter(dict[str, InstanceRequest]).validate_python(yaml.safe_load(raw_data))
    timeseries_external_ids: set[str] = set()
    for key in ["checkListReading", "templateReading"]:
        reading = instances[key]
        if not reading.sources:
            continue
        source = reading.sources[0]
        if source.properties and "timeseries" in source.properties:
            timeseries_external_ids.update(source.properties["timeseries"])  # type: ignore[arg-type]
    checklist_item = instances["checklistItem"]
    file_external_ids: set[str] = set()
    if (
        checklist_item.sources
        and checklist_item.sources[0].properties
        and "files" in checklist_item.sources[0].properties
    ):
        file_external_ids.update(checklist_item.sources[0].properties["files"])  # type: ignore[arg-type]

    return (
        list(instances.values()),
        [
            TimeSeriesRequest(
                external_id=external_id,
            )
            for external_id in timeseries_external_ids
        ],
        [
            FileMetadataRequest(
                external_id=external_id,
                name=external_id,
            )
            for external_id in file_external_ids
        ],
    )
