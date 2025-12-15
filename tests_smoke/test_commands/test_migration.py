import time
from collections.abc import Iterator
from pathlib import Path
from typing import cast

import pytest
from cognite.client.data_classes import (
    Asset,
    AssetWrite,
    DataSet,
    FileMetadata,
    ThreeDModelRevision,
    ThreeDModelRevisionWrite,
    filters,
)
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeOrEdgeData, Space, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.three_d import (
    AssetMappingClassicRequest,
    ThreeDModelClassicRequest,
    ThreeDModelResponse,
)
from cognite_toolkit._cdf_tk.commands import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper, ThreeDMapper
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL, SPACE_SOURCE_VIEW_ID
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import ASSET_ID
from cognite_toolkit._cdf_tk.commands._migrate.issues import ThreeDModelMigrationIssue
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import AssetCentricMigrationIO, ThreeDMigrationIO
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.storageio import UploadItem
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.http_client import FailedRequestMessage, FailedResponse, HTTPClient
from tests.test_integration.constants import RUN_UNIQUE_ID
from tests_smoke.exceptions import EndpointAssertionError


@pytest.fixture
def tmp_classic_asset(toolkit_client: ToolkitClient, smoke_dataset: DataSet) -> Iterator[Asset]:
    client = toolkit_client
    asset = AssetWrite(
        name=f"toolkit_classic_asset_migration_test_{RUN_UNIQUE_ID}",
        data_set_id=smoke_dataset.id,
        metadata={"source": "smoke_test_migration"},
        external_id=f"toolkit_classic_asset_migration_test_{RUN_UNIQUE_ID}",
    )
    client.assets.delete(external_id=asset.external_id, ignore_unknown_ids=True)
    created = client.assets.create(asset)
    yield created

    client.assets.delete(external_id=asset.external_id, ignore_unknown_ids=True)


@pytest.fixture
def migrated_asset(
    toolkit_client: ToolkitClient, tmp_classic_asset: Asset, smoke_space: Space, tmp_path: Path
) -> Iterator[tuple[Asset, Node]]:
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
        selected=MigrationCSVFileSelector(datafile=csv_file, kind="Assets"),
        data=AssetCentricMigrationIO(client),
        mapper=AssetCentricMapper(client),
        log_dir=tmp_path / "migration_logs",
        dry_run=False,
        verbose=False,
    )
    asset_external_id = cast(str, asset.external_id)
    migrated_nodes = client.data_modeling.instances.retrieve((smoke_space.space, asset_external_id)).nodes
    if not migrated_nodes:
        raise EndpointAssertionError(
            client.data_modeling.instances._RESOURCE_PATH,
            "Failed to retrieve migrated asset instance from data modeling.",
        )
    yield tmp_classic_asset, migrated_nodes[0]

    client.data_modeling.instances.delete((smoke_space.space, asset_external_id))


@pytest.fixture
def tmp_3D_model_with_asset_mapping(
    toolkit_client: ToolkitClient,
    three_d_file: FileMetadata,
    smoke_dataset: DataSet,
    smoke_space: Space,
    migrated_asset: tuple[Asset, Node],
) -> Iterator[ThreeDModelResponse]:
    classic_asset, _ = migrated_asset
    client = toolkit_client
    model_request = ThreeDModelClassicRequest(
        name=f"toolkit_3d_model_migration_test_{RUN_UNIQUE_ID}",
        dataSetId=smoke_dataset.id,
        metadata={"source": "smoke_test_migration"},
    )
    models = client.tool.three_d.models.create([model_request])
    if len(models) != 1:
        raise EndpointAssertionError(
            client.tool.three_d.models.ENDPOINT, "Failed to create 3D model for migration test."
        )
    model = models[0]

    revision = client.three_d.revisions.create(
        model.id, ThreeDModelRevisionWrite(file_id=three_d_file.id, published=True)
    )
    if not isinstance(revision, ThreeDModelRevision):
        raise EndpointAssertionError(
            client.three_d.revisions._RESOURCE_PATH, "Failed to create 3D model revision for migration test."
        )

    max_time = time.time() + 300  # 5 minutes timeout
    while revision.status in {"Processing", "Queued"}:
        revision_status = client.three_d.revisions.retrieve(model.id, revision.id)
        if revision_status is None:
            raise EndpointAssertionError(
                client.three_d.revisions._RESOURCE_PATH,
                "Failed to retrieve 3D model revision status for migration test.",
            )
        revision = revision_status
        time.sleep(1)
        if time.time() > max_time:
            raise AssertionError("Timeout waiting for 3D model revision to be processed.")
    if revision.status != "Done":
        raise AssertionError(f"3D model revision processing failed with status: {revision.status}")
    page = client.tool.three_d.models.iterate(include_revision_info=True)
    retrieved_model = next((m for m in page.items if m.id == model.id), None)
    if not retrieved_model:
        raise EndpointAssertionError(
            client.tool.three_d.models.ENDPOINT, "Failed to retrieve created 3D model for migration test."
        )
    if retrieved_model.last_revision_info is None or retrieved_model.last_revision_info.revision_id is None:
        raise AssertionError("Retrieved 3D model has incorrect revision info.")
    three_d_nodes = client.three_d.revisions.list_nodes(
        retrieved_model.id, revision_id=retrieved_model.last_revision_info.revision_id, limit=1
    )
    if not three_d_nodes:
        raise EndpointAssertionError(
            client.three_d.revisions._RESOURCE_PATH,
            "Failed to verify 3D model revision has nodes for migration test.",
        )
    three_d_node = three_d_nodes[0]
    if not three_d_node.id:
        raise AssertionError("3D model node has no ID.")
    created_mapping = client.tool.three_d.asset_mappings.create(
        [
            AssetMappingClassicRequest(
                nodeId=three_d_node.id,
                assetId=classic_asset,
                modelId=model.id,
                revisionId=revision.id,
            )
        ]
    )
    if not created_mapping or len(created_mapping) != 1:
        raise EndpointAssertionError(
            client.tool.three_d.asset_mappings.ENDPOINT,
            "Failed to create asset mapping for 3D model migration test.",
        )

    yield retrieved_model

    client.tool.three_d.models.delete([model.id])
    client.data_modeling.instances.delete(
        # Delete both model and revision instances
        [(smoke_space.space, f"cog_3d_model_{model.id!s}"), (smoke_space.space, f"cog_3d_revision_{revision.id!s}")]
    )


@pytest.fixture(scope="session")
def three_d_model_instance_space(toolkit_client: ToolkitClient, smoke_space: Space, smoke_dataset: DataSet) -> None:
    """This sets up the instance space mapping from the classic dataset."""
    client = toolkit_client
    space = smoke_space.space
    client.data_modeling.instances.apply(
        NodeApply(
            space=COGNITE_MIGRATION_MODEL.space,
            external_id=space,
            sources=[
                NodeOrEdgeData(
                    source=SPACE_SOURCE_VIEW_ID,
                    properties={
                        "instanceSpace": space,
                        "dataSetId": smoke_dataset.id,
                        "dataSetExternalId": smoke_dataset.external_id,
                    },
                )
            ],
        )
    )


class TestMigrate3D:
    ERROR_HEADING = "3D model migration failed. "

    @pytest.mark.usefixtures("three_d_model_instance_space")
    def test_migrate_3d_model(
        self,
        tmp_3D_model_with_asset_mapping: ThreeDModelResponse,
        toolkit_client: ToolkitClient,
        tmp_path: Path,
        smoke_space: Space,
    ) -> None:
        client = toolkit_client
        model = tmp_3D_model_with_asset_mapping
        if model.last_revision_info is None:
            raise AssertionError(f"{self.ERROR_HEADING}3D model has no revision info.")

        mapper = ThreeDMapper(client)

        mapped = mapper.map([model])
        if len(mapped) != 1:
            raise AssertionError(f"{self.ERROR_HEADING}Failed to map classic 3D to data modeling format.")
        migration_request, issue = mapped[0]
        if not isinstance(issue, ThreeDModelMigrationIssue):
            raise AssertionError(f"{self.ERROR_HEADING}Issue object not of expected type got {type(issue)}.")
        if issue.has_issues:
            raise AssertionError(f"{self.ERROR_HEADING}Issues: {humanize_collection(issue.error_message)}")
        if migration_request is None:
            raise AssertionError(f"{self.ERROR_HEADING}Mapped migration request is None.")
        io = ThreeDMigrationIO(client)

        with HTTPClient(config=client.config) as http_client:
            result = io.upload_items(
                [UploadItem(source_id=str(model.id), item=migration_request)], http_client=http_client
            )

        errors = [str(res) for res in result if isinstance(res, FailedResponse | FailedRequestMessage)]
        if len(errors) > 0:
            raise EndpointAssertionError(
                io.UPLOAD_ENDPOINT, f"{self.ERROR_HEADING}Errors: {humanize_collection(errors)}"
            )

        view_id = ViewId("cdf_cdm", "Cognite3DModel", "v1")
        has_name = filters.Equals(view_id.as_property_ref("name"), model.name)
        nodes = client.data_modeling.instances.list(
            instance_type="node",
            sources=[ViewId("cdf_cdm", "Cognite3DModel", "v1")],
            space=smoke_space.space,
            filter=has_name,
        )
        if len(nodes) != 1:
            raise EndpointAssertionError(
                client.data_modeling.instances._RESOURCE_PATH,
                f"{self.ERROR_HEADING}. 3D model instance not found in data modeling after migration.",
            )

        migrated_model = nodes[0]
        if not migrated_model.external_id.endswith(str(model.id)):
            raise AssertionError(f"{self.ERROR_HEADING}Migrated 3D model ID does not match expected format.")

        revision_view = ViewId("cdf_cdm", "Cognite3DRevision", "v1")
        has_model_id = filters.Equals(
            revision_view.as_property_ref("model3D"), migrated_model.as_id().dump(include_instance_type=False)
        )
        revisions = client.data_modeling.instances.list(
            instance_type="node",
            sources=[revision_view],
            space=smoke_space.space,
            filter=has_model_id,
        )
        if len(revisions) != 1:
            raise EndpointAssertionError(
                client.data_modeling.instances._RESOURCE_PATH,
                f"{self.ERROR_HEADING}3D revision instance not found in data modeling after migration.",
            )
        migrated_revision = revisions[0]
        if not migrated_revision.external_id.endswith(str(model.last_revision_info.revision_id)):
            raise AssertionError(f"{self.ERROR_HEADING}Migrated 3D revision ID does not match expected format.")
