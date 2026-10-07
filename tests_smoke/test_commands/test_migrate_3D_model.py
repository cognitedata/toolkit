import time
from collections.abc import Iterator
from pathlib import Path
from typing import cast
from unittest.mock import MagicMock, patch

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    DataSet,
    filters,
)
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteAsset

from cognite_toolkit._cdf_tk.apps import MigrateApp
from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.request_classes.filters import InstanceFilter
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceSource,
    NodeRequest,
    NodeResponse,
    SpaceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicRequestId,
    ThreeDModelClassicRequest,
    ThreeDModelClassicResponse,
    ThreeDRevisionClassicRequest,
    ThreeDRevisionClassicResponse,
)
from cognite_toolkit._cdf_tk.commands import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import (
    AssetCentricToInstanceMapper,
)
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL, SPACE_SOURCE_VIEW_ID
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import ASSET_ID
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import (
    AssetCentricMigrationIO,
)
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from tests.test_integration.constants import RUN_UNIQUE_ID
from tests_smoke.exceptions import EndpointAssertionError


@pytest.fixture
def load_toolkit_client(toolkit_client: ToolkitClient) -> Iterator[None]:
    """Make the typer commands use the smoke-test toolkit client instead of loading it from the environment."""
    with patch(f"{MigrateApp.__module__}.EnvironmentVariables") as mock_env:
        mock_env.create_from_environment.return_value.get_client.return_value = toolkit_client
        yield


@pytest.fixture
def tmp_classic_asset(toolkit_client: ToolkitClient, smoke_dataset: DataSet) -> Iterator[AssetResponse]:
    client = toolkit_client
    external_id = f"toolkit_classic_asset_migration_test_{RUN_UNIQUE_ID}"
    asset = AssetRequest(
        name=external_id,
        data_set_id=smoke_dataset.id,
        metadata={"source": "smoke_test_migration"},
        external_id=external_id,
    )
    client.tool.assets.delete([ExternalId(external_id=external_id)], ignore_unknown_ids=True)
    created = client.tool.assets.create([asset])[0]
    yield created

    client.tool.assets.delete([ExternalId(external_id=external_id)], ignore_unknown_ids=True)


@pytest.fixture
def migrated_asset(
    toolkit_client: ToolkitClient, tmp_classic_asset: AssetResponse, smoke_space: SpaceResponse, tmp_path: Path
) -> Iterator[tuple[AssetResponse, NodeResponse]]:
    if not tmp_classic_asset.id or not tmp_classic_asset.external_id or not tmp_classic_asset.data_set_id:
        raise AssertionError("Temporary classic asset is missing required fields for migration test.")
    asset = tmp_classic_asset
    csv_file = tmp_path / "asset_mapping.csv"
    with open(csv_file, "w") as f:
        f.write("externalId,space,id,dataSetId,ingestionView\n")
        f.write(f"{asset.external_id},{smoke_space.space},{asset.id},{asset.data_set_id},{ASSET_ID}\n")

    client = toolkit_client
    cmd = MigrationCommand()
    cmd.migrate(
        selectors=[MigrationCSVFileSelector(datafile=csv_file, kind="Assets")],
        data=AssetCentricMigrationIO(client),
        mapper=AssetCentricToInstanceMapper(client),
        log_dir=tmp_path / "migration_logs",
        dry_run=False,
        verbose=False,
    )
    asset_external_id = cast(str, asset.external_id)
    migrated_nodes = client.tool.instances.retrieve([NodeId(space=smoke_space.space, external_id=asset_external_id)])
    migrated_node = migrated_nodes[0] if migrated_nodes else None
    if not isinstance(migrated_node, NodeResponse):
        raise EndpointAssertionError(
            "data_modeling.instances.retrieve",
            "Failed to retrieve migrated asset instance from data modeling.",
        )
    yield tmp_classic_asset, migrated_node

    client.tool.instances.delete([NodeId(space=smoke_space.space, external_id=asset_external_id)])


@pytest.fixture
def tmp_3D_model_with_asset_mapping(
    toolkit_client: ToolkitClient,
    three_d_file: FileMetadataResponse,
    smoke_dataset: DataSet,
    smoke_space: SpaceResponse,
    migrated_asset: tuple[AssetResponse, NodeResponse],
) -> Iterator[tuple[ThreeDModelClassicResponse, NodeResponse]]:
    classic_asset, asset_node = migrated_asset
    client = toolkit_client
    model_request = ThreeDModelClassicRequest(
        name=f"toolkit_3d_model_migration_test_{RUN_UNIQUE_ID}",
        data_set_id=smoke_dataset.id,
        metadata={"source": "smoke_test_migration"},
    )
    models = client.tool.three_d.models_classic.create([model_request])
    if len(models) != 1:
        create_path = client.tool.three_d.models_classic._method_endpoint_map["create"].path
        raise EndpointAssertionError(create_path, "Failed to create 3D model for migration test.")
    model = models[0]

    created_revisions = client.tool.three_d.revisions_classic.create(
        [ThreeDRevisionClassicRequest(model_id=model.id, file_id=three_d_file.id, published=True)]
    )
    if len(created_revisions) != 1:
        raise EndpointAssertionError("three_d.revisions", "Failed to create 3D model revision for migration test.")
    revision: ThreeDRevisionClassicResponse = created_revisions[0]

    max_time = time.time() + 300  # 5 minutes timeout
    while revision.status in {"Processing", "Queued"}:
        revisions = client.tool.three_d.revisions_classic.list(model.id, limit=None)
        revision_status = next((item for item in revisions if item.id == revision.id), None)
        if revision_status is None:
            raise EndpointAssertionError(
                "three_d.revisions",
                "Failed to retrieve 3D model revision status for migration test.",
            )
        revision = revision_status
        time.sleep(1)
        if time.time() > max_time:
            raise AssertionError("Timeout waiting for 3D model revision to be processed.")
    if revision.status != "Done":
        raise AssertionError(f"3D model revision processing failed with status: {revision.status}")
    page = client.tool.three_d.models_classic.paginate(include_revision_info=True)
    retrieved_model = next((m for m in page.items if m.id == model.id), None)
    if not retrieved_model:
        list_path = client.tool.three_d.models_classic._method_endpoint_map["list"].path
        raise EndpointAssertionError(list_path, "Failed to retrieve created 3D model for migration test.")
    if retrieved_model.last_revision_info is None or retrieved_model.last_revision_info.revision_id is None:
        raise AssertionError("Retrieved 3D model has incorrect revision info.")
    three_d_nodes = client.three_d.revisions.list_nodes(
        retrieved_model.id, revision_id=retrieved_model.last_revision_info.revision_id, limit=1
    )
    if not three_d_nodes:
        raise EndpointAssertionError(
            "three_d.revisions",
            "Failed to verify 3D model revision has nodes for migration test.",
        )
    three_d_node = three_d_nodes[0]
    if not three_d_node.id:
        raise AssertionError("3D model node has no ID.")
    created_mapping = client.tool.three_d.asset_mappings_classic.create(
        [
            AssetMappingClassicRequestId(
                node_id=three_d_node.id,
                asset_id=classic_asset.id,
                model_id=model.id,
                revision_id=revision.id,
            )
        ]
    )
    if not created_mapping or len(created_mapping) != 1:
        raise EndpointAssertionError(
            client.tool.three_d.asset_mappings_classic.ENDPOINT,
            "Failed to create asset mapping for 3D model migration test.",
        )

    yield retrieved_model, asset_node

    client.tool.three_d.models_classic.delete([model.as_request_resource().as_id()])
    client.tool.instances.delete(
        [
            NodeId(space=smoke_space.space, external_id=f"cog_3d_model_{model.id!s}"),
            NodeId(space=smoke_space.space, external_id=f"cog_3d_revision_{revision.id!s}"),
        ]
    )


@pytest.fixture(scope="session")
def three_d_model_instance_space(
    toolkit_client: ToolkitClient, smoke_space: SpaceResponse, smoke_dataset: DataSet
) -> None:
    """This sets up the instance space mapping from the classic dataset."""
    client = toolkit_client
    space = smoke_space.space
    client.tool.instances.create(
        [
            NodeRequest(
                space=COGNITE_MIGRATION_MODEL.space,
                external_id=space,
                sources=[
                    InstanceSource(
                        source=SPACE_SOURCE_VIEW_ID,
                        properties={
                            "instanceSpace": space,
                            "dataSetId": smoke_dataset.id,
                            "dataSetExternalId": smoke_dataset.external_id,
                        },
                    )
                ],
            )
        ],
        replace=True,
    )


class TestMigrate3D:
    ERROR_HEADING = "3D model migration failed. "

    @pytest.mark.usefixtures("three_d_model_instance_space", "load_toolkit_client")
    def test_migrate_3d_model_then_migrate_asset_mapping(
        self,
        tmp_3D_model_with_asset_mapping: tuple[ThreeDModelClassicResponse, NodeResponse],
        toolkit_client: ToolkitClient,
        tmp_path: Path,
        smoke_space: SpaceResponse,
    ) -> None:
        # --- Setup (mostly done by fixtures) --------------------------------
        client = toolkit_client
        model, asset_node = tmp_3D_model_with_asset_mapping
        if model.last_revision_info is None:
            raise AssertionError(f"{self.ERROR_HEADING}3D model has no revision info.")

        # --- Act: migrate the 3D model and its asset mappings ---------------
        MigrateApp.three_d(
            ctx=MagicMock(),
            cdf_project=client.config.project,
            id=[model.id],
            log_dir=tmp_path / "three_d_models",
            dry_run=False,
            verbose=True,
        )

        MigrateApp.three_d_asset_mapping(
            ctx=MagicMock(),
            cdf_project=client.config.project,
            model_id=[model.id],
            object_3D_space=smoke_space.space,
            cad_node_space=smoke_space.space,
            log_dir=tmp_path / "three_d_asset_mapping",
            dry_run=False,
            verbose=True,
        )

        # --- Assert: the migration created the expected data modeling nodes -
        # Validate that the model exists in data modeling
        view_id = ViewId(space="cdf_cdm", external_id="Cognite3DModel", version="v1")
        has_name = filters.Equals(view_id.as_property_reference("name"), model.name)
        nodes = [
            item
            for item in client.tool.instances.list(
                filter=InstanceFilter(
                    instance_type="node",
                    source=view_id,
                    filter={
                        "and": [
                            has_name.dump(),
                            {"equals": {"property": ["node", "space"], "value": smoke_space.space}},
                        ]
                    },
                ),
                limit=None,
            )
            if isinstance(item, NodeResponse)
        ]
        if len(nodes) != 1:
            raise EndpointAssertionError(
                "data_modeling.instances.retrieve",
                f"{self.ERROR_HEADING}. 3D model instance not found in data modeling after migration.",
            )

        migrated_model = nodes[0]
        if not migrated_model.external_id.endswith(str(model.id)):
            raise AssertionError(f"{self.ERROR_HEADING}Migrated 3D model ID does not match expected format.")

        # Validate that the revision exists in data modeling
        revision_view = ViewId(space="cdf_cdm", external_id="Cognite3DRevision", version="v1")
        has_model_id = filters.Equals(
            revision_view.as_property_reference("model3D"), migrated_model.as_id().dump(include_instance_type=False)
        )
        revisions = [
            item
            for item in client.tool.instances.list(
                filter=InstanceFilter(
                    instance_type="node",
                    source=revision_view,
                    filter={
                        "and": [
                            has_model_id.dump(),
                            {"equals": {"property": ["node", "space"], "value": smoke_space.space}},
                        ]
                    },
                ),
                limit=None,
            )
            if isinstance(item, NodeResponse)
        ]
        if len(revisions) != 1:
            raise EndpointAssertionError(
                "data_modeling.instances.retrieve",
                f"{self.ERROR_HEADING}3D revision instance not found in data modeling after migration.",
            )
        migrated_revision = revisions[0]
        if not migrated_revision.external_id.endswith(str(model.last_revision_info.revision_id)):
            raise AssertionError(f"{self.ERROR_HEADING}Migrated 3D revision ID does not match expected format.")

        # Verify that the asset mapping exists in data modeling
        cognite_asset = client.data_modeling.instances.retrieve_nodes(
            dm.NodeId(space=asset_node.space, external_id=asset_node.external_id), node_cls=CogniteAsset
        )
        if not cognite_asset:
            raise EndpointAssertionError(
                "data_modeling.instances.retrieve",
                f"{self.ERROR_HEADING}CogniteAsset instance not found in data modeling after migration.",
            )
        if cognite_asset.object_3d is None:
            raise AssertionError(f"{self.ERROR_HEADING}CogniteAsset instance has no 3D object mapping after migration.")
        object3D = cognite_asset.object_3d
        cad_node_view = ViewId(space="cdf_cdm", external_id="CogniteCADNode", version="v1")
        is_cad_node = filters.Equals(
            cad_node_view.as_property_reference("object3D"),
            {"space": object3D.space, "externalId": object3D.external_id},
        )
        cad_node = [
            item
            for item in client.tool.instances.list(
                filter=InstanceFilter(instance_type="node", source=cad_node_view, filter=is_cad_node.dump()),
                limit=None,
            )
            if isinstance(item, NodeResponse)
        ]
        if len(cad_node) != 1:
            raise EndpointAssertionError(
                "data_modeling.instances.retrieve",
                f"{self.ERROR_HEADING}CAD node instance not found in data modeling after migration.",
            )
