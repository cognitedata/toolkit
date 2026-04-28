import time
from collections import defaultdict
from collections.abc import Callable, Iterator
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
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import InstanceId, NodeId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigRequest,
    FeatureConfiguration,
    RootLocationConfiguration,
)
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceRequest,
    InstanceSource,
    NodeRequest,
    SpaceRequest,
    SpaceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest
from cognite_toolkit._cdf_tk.client.resource_classes.infield import DataStorage, InFieldCDMLocationConfigRequest
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest
from cognite_toolkit._cdf_tk.commands._migrate.data_model import INSTANCE_SOURCE_VIEW_ID
from cognite_toolkit._cdf_tk.commands._migrate.infield_data_mappings import (
    create_infield_data_mappings,
)
from cognite_toolkit._cdf_tk.resource_ios import ViewIO
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import NDJsonReader

THIS_DIR = Path(__file__).parent
TEST_DATA = THIS_DIR / "infield_test_data.yaml"


@pytest.fixture
def load_toolkit_client(toolkit_client: ToolkitClient) -> None:
    with patch(f"{MigrateApp.__module__}.EnvironmentVariables") as mock_env:
        mock_env.create_from_environment.return_value.get_client.return_value = toolkit_client


@pytest.fixture
def source_space(smoke_space: SpaceResponse) -> SpaceResponse:
    """Simulates the space where InField legacy has written data. This data needs to be migrated to the target space."""
    return smoke_space


@pytest.fixture()
def read_space(toolkit_client: ToolkitClient) -> SpaceResponse:
    """This is where InField legacy is reading data from. This is the where the asset are stored.
    InField treat this as a read-only space.
    """
    client = toolkit_client
    target_space = "smoke_infield_migration_read_space"
    if spaces := client.tool.spaces.retrieve([SpaceId(space=target_space)]):
        return spaces[0]
    return client.tool.spaces.create(
        [
            SpaceRequest(
                name="Smoke Infield Migration Read Space",
                space=target_space,
                description="Space for reading InField legacy data during migration smoke test",
            )
        ]
    )[0]


@pytest.fixture()
def target_space(toolkit_client: ToolkitClient) -> SpaceResponse:
    """This is the space where InFieldOnCDM will write data to. This is the space we are migrating to."""
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


@pytest.fixture()
def infield_legacy(
    toolkit_client: ToolkitClient, source_space: SpaceResponse, read_space: SpaceResponse, target_space: SpaceResponse
) -> Iterator[list[InstanceRequest]]:
    client = toolkit_client

    myself = client.user_profiles.me()
    instances, timeseries, files, asset = load_infield_source_data(
        user_id=myself.user_identifier, source_space=source_space.space, read_space=read_space.space
    )
    instances.append(
        NodeRequest(
            space="cognite_app_data",
            external_id=myself.user_identifier,
            sources=[
                InstanceSource(
                    source=ViewId(space="cdf_apps_shared", external_id="CDF_User", version="v1"),
                    properties={
                        "email": "example@email.com",
                        "name": "Example User",
                    },
                )
            ],
        )
    )

    for ts in timeseries:
        instances.append(
            NodeRequest(
                space=target_space.space,
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
                space=target_space.space,
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

    asset_external_id = cast(str, asset.external_id)
    migrated_asset = NodeRequest(
        space=target_space.space,
        external_id=asset_external_id,
        sources=[
            InstanceSource(
                source=ViewId(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
                properties={"name": asset.name},
            ),
            # Lineage of migration, used in InField migration.
            InstanceSource(
                source=INSTANCE_SOURCE_VIEW_ID,
                properties={
                    "resourceType": "asset",
                    "id": -1,  # Ignored by InField
                    "classicExternalId": asset_external_id,
                },
            ),
        ],
    )
    instances.append(migrated_asset)

    to_create_by_view_id: dict[ViewId, list[InstanceRequest]] = defaultdict(list)
    edges: list[InstanceRequest] = []
    for instance in instances:
        if not instance.sources:
            edges.append(instance)
            continue
        source = instance.sources[0]
        if isinstance(source.source, ViewId):
            to_create_by_view_id[source.source].append(instance)
    sorted_views, _ = ViewIO(toolkit_client, None, None).topological_sort_container_constraints(
        list(to_create_by_view_id.keys())
    )

    # Ensure clean state
    # Cleanup
    deleted = client.tool.instances.delete([item.as_id() for item in instances])

    if deleted:
        time.sleep(5)

    #### Deploy instances to legacy InField model #####
    for view_id in sorted_views:
        instance_batch = to_create_by_view_id[view_id]
        try:
            _ = client.tool.instances.create(instance_batch, replace=True)
        except ToolkitAPIError as e:
            raise AssertionError(
                f"Failed to create instance batch for view {view_id!s}. Error: {e}. Batch: {[item.as_id() for item in instance_batch]}"
            ) from e
    if edges:
        try:
            _ = client.tool.instances.create(edges, replace=True)
        except ToolkitAPIError as e:
            raise AssertionError(
                f"Failed to create instance batch for edges with no view. Error: {e}. Batch: {[item.as_id() for item in edges]}"
            ) from e
    #######

    #### Create 'migrated' timeseries and files #####
    # These are technically not migrated, but it is the simplest way is to create CogniteTimeSeries/CogniteFile
    # and update the classic with the externalId.from
    timeseries_updates: list[TimeSeriesUpdate] = []
    timeseries_nodes_ids: list[InstanceId] = []
    for ts in timeseries:
        external_id = cast(str, ts.external_id)
        node_id = dm.NodeId(space=target_space.space, external_id=external_id)
        ts_update = TimeSeriesUpdate(
            instance_id=node_id,
        ).external_id.set(external_id)
        timeseries_updates.append(ts_update)
        timeseries_nodes_ids.append(InstanceId(instance_id=NodeId(space=target_space.space, external_id=external_id)))
    file_updates: list[FileMetadataUpdate] = []
    file_nodes_ids: list[InstanceId] = []
    for file in files:
        external_id = cast(str, file.external_id)
        file_update = FileMetadataUpdate(
            instance_id=dm.NodeId(space=target_space.space, external_id=external_id),
        ).external_id.set(external_id)
        file_updates.append(file_update)
        file_nodes_ids.append(InstanceId(instance_id=NodeId(space=target_space.space, external_id=external_id)))

    # Ensure that the syncer has created the timeseries and files before updating.
    wait_for_resources(
        lambda: client.tool.timeseries.retrieve(timeseries_nodes_ids, ignore_unknown_ids=False), "timeseries"
    )
    _ = client.time_series.update(timeseries_updates)
    wait_for_resources(
        lambda: client.tool.filemetadata.retrieve(file_nodes_ids, ignore_unknown_ids=False), "filemetadata"
    )
    _ = client.files.update(file_updates)

    yield instances

    # Cleanup
    client.tool.instances.delete([item.as_id() for item in instances])


def wait_for_resources(api_call: Callable[[], Any], resource_name: str, timeout: float = 30) -> None:
    end_time = time.time() + timeout
    while time.time() < end_time:
        try:
            _ = api_call()
            return  # Success
        except ToolkitAPIError:
            time.sleep(1)
    raise AssertionError(f"Timed out waiting for {resource_name} to be synced.")


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
        # Create APM config (InField legacy node)
        toolkit_client.infield.apm_config.create(
            [
                APMConfigRequest(
                    name="InField Smoke Test APM Config",
                    external_id="infield_smoke_test_apm_config",
                    feature_configuration=FeatureConfiguration(
                        root_location_configurations=[
                            RootLocationConfiguration(app_data_instance_space=source_space.space)
                        ]
                    ),
                )
            ]
        )
        # Create LocationOnCDM node for InFieldOnCDM
        toolkit_client.infield.cdm_config.create(
            [
                InFieldCDMLocationConfigRequest(
                    space=target_space.space,
                    name="InField Smoke Test CDM Location Config",
                    external_id="infield_smoke_test_cdm_location_config",
                    data_storage=DataStorage(app_instance_space=target_space.space),
                )
            ]
        )

        MigrateApp.infield_data(
            ctx=MagicMock(),
            source_space=source_space.space,
            target_space=target_space.space,
            log_dir=tmp_path,
            dry_run=False,
            verbose=True,
        )

        destination_by_view_id, expected_node_count, missing_mappings = self._get_destination_nodes(
            infield_legacy, target_space
        )

        destination_instances: list[dict[str, Any]] = []
        for view_id, node_ids in destination_by_view_id.items():
            target_nodes = toolkit_client.tool.instances.retrieve(node_ids, source=view_id)
            for node in target_nodes:
                # Excluding "last_updated_time" and "version" as they will change for each run of the test.
                dumped = node.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_unset=True,
                    exclude={"last_updated_time", "version", "created_time"},
                )
                for view_properties in dumped.get("properties", {}).values():
                    for properties in view_properties.values():
                        # These also change for each run.
                        properties.pop("sourceCreatedTime", None)
                        properties.pop("sourceUpdatedTime", None)
                destination_instances.append(dumped)

        log_files = [file for file in tmp_path.rglob("*.ndjson") if file.is_file()]
        if log_files:
            print(f"Migration log files found in {tmp_path}: {[file.name for file in log_files]}")
            for log_file in log_files:
                for chunk in NDJsonReader(log_file).read_chunks():
                    print(chunk)
        else:
            print("No migration log files found. This means there were no issues.")
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

        # Cleanup for next run.
        destination_node_ids = [node_id for node_ids in destination_by_view_id.values() for node_id in node_ids]
        toolkit_client.tool.instances.delete(destination_node_ids)

        data_regression.check({"instances": destination_instances})

    def _get_destination_nodes(
        self, infield_legacy: list[InstanceRequest], target_space: SpaceResponse
    ) -> tuple[dict[ViewId, list[NodeId]], int, list[ViewId]]:
        mappings = create_infield_data_mappings()
        mapping_by_source = {item.source_view: item for item in mappings}
        destination_by_view_id: dict[ViewId, list[NodeId]] = defaultdict(list)
        missing_mappings: list[ViewId] = []
        expected_node_count = 0
        for instance in infield_legacy:
            if not isinstance(instance, NodeRequest):
                continue
            if instance.sources and instance.sources[0].source.space == "cdf_apm":
                expected_node_count += 1
            for source in instance.sources or []:
                if not isinstance(source.source, ViewId):
                    continue
                if source.source not in mapping_by_source:
                    if source.source.space == "cdf_apm":
                        missing_mappings.append(source.source)
                    continue
                mapping = mapping_by_source[source.source]
                destination_by_view_id[mapping.destination_view].append(
                    NodeId(space=target_space.space, external_id=instance.external_id)
                )
        return destination_by_view_id, expected_node_count, missing_mappings


def load_infield_source_data(
    user_id: str, source_space: str, read_space: str
) -> tuple[list[InstanceRequest], list[TimeSeriesRequest], list[FileMetadataRequest], AssetRequest]:
    raw_data = (
        TEST_DATA.read_text()
        .replace("{{userId}}", user_id)
        .replace("{{source_space}}", source_space)
        .replace("{{readSpace}}", read_space)
    )
    instances = TypeAdapter(dict[str, InstanceRequest]).validate_python(yaml.safe_load(raw_data))
    timeseries_external_ids: set[str] = set()
    for key in ["checkListReading", "templateReading"]:
        reading = instances[key]
        if not reading.sources:
            continue
        source = reading.sources[0]
        if source.properties and "timeseries" in source.properties:
            timeseries_values = source.properties["timeseries"]
            if isinstance(timeseries_values, str):
                timeseries_external_ids.add(timeseries_values)
            elif isinstance(timeseries_values, list):
                timeseries_external_ids.update(timeseries_values)  # type: ignore[arg-type]

    file_external_ids: set[str] = set()
    for key in ["checklistItem", "observation"]:
        reading = instances[key]
        if reading.sources and reading.sources[0].properties and "files" in reading.sources[0].properties:
            file_external_ids.update(reading.sources[0].properties["files"])  # type: ignore[arg-type]

    asset_instance = instances["asset"]
    asset_request = AssetRequest(
        external_id=asset_instance.external_id,
        # MyPy this is validated by the AssetRequest pydantic model.
        name=asset_instance.sources[0].properties["title"],  # type: ignore[index, arg-type]
    )

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
        asset_request,
    )
