import time
from collections import defaultdict
from collections.abc import Iterator
from pathlib import Path
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
import yaml
from cognite.client import data_modeling as dm
from cognite.client.data_classes import FileMetadataUpdate, TimeSeriesUpdate
from pydantic import TypeAdapter
from pytest_regressions.data_regression import DataRegressionFixture

from cognite_toolkit._cdf_tk.apps._migrate_app import MigrateApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import NodeId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceRequest,
    InstanceSource,
    NodeRequest,
    SpaceRequest,
    SpaceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest
from cognite_toolkit._cdf_tk.commands._migrate.infield_data_mappings import create_infield_data_mappings
from cognite_toolkit._cdf_tk.utils import humanize_collection

THIS_DIR = Path(__file__).parent
TEST_DATA = THIS_DIR / "infield_test_data.yaml"


@pytest.fixture
def load_toolkit_client(toolkit_client: ToolkitClient) -> None:
    with patch(f"{MigrateApp.__module__}.EnvironmentVariables") as mock_env:
        mock_env.create_from_environment.return_value.get_client.return_value = toolkit_client


@pytest.fixture
def source_space(smoke_space: SpaceResponse) -> SpaceResponse:
    return smoke_space


@pytest.fixture()
def infield_legacy(toolkit_client: ToolkitClient, source_space: SpaceResponse) -> Iterator[list[InstanceRequest]]:
    client = toolkit_client

    myself = client.user_profiles.me()
    instances, timeseries, files = load_infield_source_data(
        user_id=myself.user_identifier, source_space=source_space.space
    )

    for ts in timeseries:
        instances.append(
            NodeRequest(
                space=source_space.space,
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
                space=source_space.space,
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

    # Ensure clean state
    # Cleanup
    client.tool.instances.delete([item.as_id() for item in instances])

    time.sleep(5)

    _ = client.tool.instances.create(instances)

    timeseries_updates: list[TimeSeriesUpdate] = []
    for ts in timeseries:
        external_id = cast(str, ts.external_id)
        ts_update = TimeSeriesUpdate(
            instance_id=dm.NodeId(space=source_space.space, external_id=external_id),
        ).external_id.set(external_id)
        timeseries_updates.append(ts_update)
    file_updates: list[FileMetadataUpdate] = []
    for file in files:
        external_id = cast(str, file.external_id)
        file_update = FileMetadataUpdate(
            instance_id=dm.NodeId(space=source_space.space, external_id=external_id),
        ).external_id.set(external_id)
        file_updates.append(file_update)
    _ = client.time_series.update(timeseries_updates)
    _ = client.files.update(file_updates)

    yield instances

    # Cleanup
    client.tool.instances.delete([item.as_id() for item in instances])


@pytest.fixture()
def target_space(toolkit_client: ToolkitClient) -> SpaceResponse:
    client = toolkit_client
    target_space = "smoke_infield_migration_target_space"
    if spaces := client.tool.spaces.retrieve([SpaceId(space=target_space)]):
        return spaces[0]
    return client.tool.spaces.create(
        [
            SpaceRequest(
                name="Smoke Infield Migration Target Space",
                space=target_space,
                description="Target space for infield migration smoke test",
            )
        ]
    )[0]


class TestMigrateInfield:
    @pytest.mark.usefixtures("load_toolkit_client")
    def test_migrate_data(
        self,
        infield_legacy: list[InstanceRequest],
        toolkit_client: ToolkitClient,
        source_space: SpaceResponse,
        target_space: SpaceResponse,
        tmp_path: Path,
        data_regression: DataRegressionFixture,
    ) -> None:
        MigrateApp.infield_data(
            ctx=MagicMock(),
            source_space=source_space.space,
            target_space=target_space.space,
            log_dir=tmp_path,
            dry_run=False,
            verbose=True,
        )

        mappings = create_infield_data_mappings()
        mapping_by_source = {item.source_view: item for item in mappings}
        # We do not any edges in the destination data, all should have been converted to direct relations.
        destination_by_view_id: dict[ViewId, list[NodeId]] = defaultdict(list)
        missing_mappings: list[ViewId] = []
        expected_node_count = 0
        for instance in infield_legacy:
            if not isinstance(instance, NodeRequest):
                continue
            expected_node_count += 1
            for source in instance.sources or []:
                if not isinstance(source.source, ViewId):
                    continue
                if source.source not in mapping_by_source:
                    missing_mappings.append(source.source)
                    continue
                mapping = mapping_by_source[source.source]
                destination_by_view_id[mapping.destination_view].append(
                    NodeId(space=target_space.space, external_id=instance.external_id)
                )

        destination_instances: list[dict[str, Any]] = []
        for view_id, node_ids in destination_by_view_id.items():
            target_nodes = toolkit_client.tool.instances.retrieve(node_ids, source=view_id)
            destination_instances.extend([node.dump() for node in target_nodes])

        if expected_node_count != len(destination_instances):
            raise AssertionError(
                f"InField migration failed. Expected {expected_node_count} nodes in destination, but found {len(destination_instances)}. Missing mappings: {humanize_collection(missing_mappings)}"
            )
        if missing_mappings:
            raise AssertionError(
                f"InField migration failed. Missing mappings for source views: {humanize_collection(missing_mappings)}"
            )

        raw_data = yaml.safe_dump(destination_instances)
        if source_space.space in raw_data:
            raise AssertionError(
                "InField migration failed. Found source space identifier in destination data, indicating that some instances were not migrated correctly."
            )

        data_regression.check(
            {
                "instances": destination_instances,
            }
        )


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
