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
from cognite.client.data_classes.data_modeling import Node, NodeApply, NodeId, NodeOrEdgeData, Space, ViewId
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteAsset

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import (
    FailedRequestMessage,
    FailedResponse,
    HTTPClient,
    RequestMessage2,
    SuccessResponse2,
    ToolkitAPIError,
)
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    AssetMappingClassicRequest,
    ThreeDModelClassicRequest,
    ThreeDModelResponse,
)
from cognite_toolkit._cdf_tk.commands import MigrationCommand
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import AssetCentricMapper, ThreeDAssetMapper, ThreeDMapper
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL, SPACE_SOURCE_VIEW_ID
from cognite_toolkit._cdf_tk.commands._migrate.default_mappings import ASSET_ID
from cognite_toolkit._cdf_tk.commands._migrate.issues import ThreeDModelMigrationIssue
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import (
    AssetCentricMigrationIO,
    ThreeDAssetMappingMigrationIO,
    ThreeDMigrationIO,
)
from cognite_toolkit._cdf_tk.commands._migrate.selectors import MigrationCSVFileSelector
from cognite_toolkit._cdf_tk.storageio import UploadItem
from cognite_toolkit._cdf_tk.storageio.selectors import ThreeDModelIdSelector
from cognite_toolkit._cdf_tk.utils import humanize_collection
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
) -> Iterator[tuple[ThreeDModelResponse, Node]]:
    classic_asset, asset_node = migrated_asset
    client = toolkit_client
    model_request = ThreeDModelClassicRequest(
        name=f"toolkit_3d_model_migration_test_{RUN_UNIQUE_ID}",
        data_set_id=smoke_dataset.id,
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
    page = client.tool.three_d.models.paginate(include_revision_info=True)
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
                node_id=three_d_node.id,
                asset_id=classic_asset.id,
                model_id=model.id,
                revision_id=revision.id,
            )
        ]
    )
    if not created_mapping or len(created_mapping) != 1:
        raise EndpointAssertionError(
            client.tool.three_d.asset_mappings.ENDPOINT,
            "Failed to create asset mapping for 3D model migration test.",
        )

    yield retrieved_model, asset_node

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
    def test_migrate_3d_model_then_migrate_asset_mapping(
        self,
        tmp_3D_model_with_asset_mapping: tuple[ThreeDModelResponse, Node],
        toolkit_client: ToolkitClient,
        tmp_path: Path,
        smoke_space: Space,
    ) -> None:
        client = toolkit_client
        model, asset_node = tmp_3D_model_with_asset_mapping
        if model.last_revision_info is None:
            raise AssertionError(f"{self.ERROR_HEADING}3D model has no revision info.")

        mapper = ThreeDMapper(client)

        # Map the classic 3D model to data modeling format
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

        # Call migration endpoint for 3D model and revision
        with HTTPClient(config=client.config) as http_client:
            result = io.upload_items(
                [UploadItem(source_id=str(model.id), item=migration_request)], http_client=http_client
            )

        errors = [str(res) for res in result if isinstance(res, FailedResponse | FailedRequestMessage)]
        if len(errors) > 0:
            raise EndpointAssertionError(
                io.UPLOAD_ENDPOINT, f"{self.ERROR_HEADING}Errors: {humanize_collection(errors)}"
            )

        # Validate that the model and revision exist in data modeling
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

        # Migrate all asset mappings for the 3D model revision
        mapping_io = ThreeDAssetMappingMigrationIO(client, smoke_space.space, smoke_space.space)
        selector = ThreeDModelIdSelector(ids=(model.id,))
        mappings = list(mapping_io.stream_data(selector=selector))
        if not mappings:
            raise AssertionError(f"{self.ERROR_HEADING}No asset mappings found for migration.")
        asset_mappings_dm = ThreeDAssetMapper(client).map([item for page in mappings for item in page.items])
        if len(asset_mappings_dm) != 1:
            raise AssertionError(f"{self.ERROR_HEADING}Failed to map asset mappings for migration.")
        asset_mapping, mapping_issue = asset_mappings_dm[0]
        if not isinstance(mapping_issue, ThreeDModelMigrationIssue):
            raise AssertionError(f"{self.ERROR_HEADING}Issue object not of expected type got {type(mapping_issue)}.")
        if mapping_issue.has_issues:
            raise AssertionError(
                f"{self.ERROR_HEADING}Mapping Issues: {humanize_collection(mapping_issue.error_message)}"
            )
        if asset_mapping is None:
            raise AssertionError(f"{self.ERROR_HEADING}Mapped asset mapping is None.")

        with HTTPClient(config=client.config) as http_client:
            mapping_results = mapping_io.upload_items(
                [UploadItem(source_id=f"{model.id}", item=asset_mapping)], http_client=http_client
            )
        mapping_errors = [str(res) for res in mapping_results if isinstance(res, FailedResponse | FailedRequestMessage)]
        if len(mapping_errors) > 0:
            raise EndpointAssertionError(
                mapping_io.UPLOAD_ENDPOINT, f"{self.ERROR_HEADING}Mapping Errors: {humanize_collection(mapping_errors)}"
            )

        # Verify that the asset mapping exists in data modeling
        cognite_asset = client.data_modeling.instances.retrieve_nodes(asset_node.as_id(), node_cls=CogniteAsset)
        if not cognite_asset:
            raise EndpointAssertionError(
                client.data_modeling.instances._RESOURCE_PATH,
                f"{self.ERROR_HEADING}CogniteAsset instance not found in data modeling after migration.",
            )
        if cognite_asset.object_3d is None:
            raise AssertionError(f"{self.ERROR_HEADING}CogniteAsset instance has no 3D object mapping after migration.")
        object3D = cognite_asset.object_3d
        cad_node_view = ViewId("cdf_cdm", "CogniteCADNode", "v1")
        is_cad_node = filters.Equals(
            cad_node_view.as_property_ref("object3D"),
            {"space": object3D.space, "externalId": object3D.external_id},
        )
        cad_node = client.data_modeling.instances.list(
            instance_type="node",
            sources=[cad_node_view],
            filter=is_cad_node,
        )
        if len(cad_node) != 1:
            raise EndpointAssertionError(
                client.data_modeling.instances._RESOURCE_PATH,
                f"{self.ERROR_HEADING}CAD node instance not found in data modeling after migration.",
            )


@pytest.fixture()
def classic_file_with_content(
    toolkit_client: ToolkitClient, smoke_dataset: DataSet, smoke_space: Space
) -> Iterator[FileMetadataResponse]:
    client = toolkit_client
    mime_type = "text/plain"
    external_id = "toolkit_classic_file_with_content_migration_smoke_test"
    metadata = FileMetadataRequest(
        external_id=external_id,
        name="Toolkit Classic File With Content Migration Smoke Test.txt",
        data_set_id=smoke_dataset.id,
        mime_type=mime_type,
    )
    # Ensure clean state
    client.data_modeling.instances.delete((smoke_space.space, external_id))
    client.tool.filemetadata.delete([metadata.as_id()], ignore_unknown_ids=True)

    created = client.tool.filemetadata.create([metadata])
    if len(created) != 1:
        raise EndpointAssertionError(
            client.tool.filemetadata._method_endpoint_map["create"].path,
            "Failed to create classic file metadata for migration test.",
        )
    created_file = created[0]
    if created_file.upload_url is None:
        raise AssertionError("Created classic file metadata has no upload URL.")
    response = client.http_client.request_single_retries(
        RequestMessage2(
            endpoint_url=created_file.upload_url,
            method="PUT",
            content_type=mime_type,
            data_content=b"Toolkit classic file content for migration smoke test.",
        )
    )
    if not isinstance(response, SuccessResponse2):
        raise EndpointAssertionError(
            created_file.upload_url,
            f"Failed to upload content for classic file metadata. Response: {response}",
        )

    yield created_file

    # Cleanup
    client.data_modeling.instances.delete((smoke_space.space, external_id))
    try:
        client.tool.filemetadata.delete([created_file.as_id()], ignore_unknown_ids=True)
    except ToolkitAPIError as e:
        if "files with instance ids must be deleted through data modeling" in str(e).lower():
            return
        raise


class TestMigrateFile:
    def test_migrate_file(
        self,
        classic_file_with_content: FileMetadataResponse,
        toolkit_client: ToolkitClient,
        smoke_dataset: DataSet,
        smoke_space: Space,
        tmp_path: Path,
    ) -> None:
        client = toolkit_client
        file = classic_file_with_content
        space = smoke_space.space
        input_file = tmp_path / "file_migration.csv"

        with input_file.open("w", encoding="utf-8") as f:
            f.write(
                "id,dataSetId,space,externalId\n"
                + "\n".join(f"{f.id},{f.data_set_id or ''},{space},{f.external_id}" for f in [file])
                + "\n"
            )

        cmd = MigrationCommand()
        cmd.migrate(
            selected=MigrationCSVFileSelector(kind="FileMetadata", datafile=input_file),
            data=AssetCentricMigrationIO(client, skip_linking=False),
            mapper=AssetCentricMapper(client),
            log_dir=tmp_path / "migration_logs",
            dry_run=False,
            verbose=False,
        )

        assert file.external_id is not None, "File external ID is None, cannot validate migration."
        # Validate that the file exists in data modeling and has content.
        # In addition, check that the instanceId is set on the file metadata in CDF.
        nodes = client.data_modeling.instances.retrieve((space, file.external_id)).nodes
        if len(nodes) != 1:
            raise EndpointAssertionError(
                client.data_modeling.instances._RESOURCE_PATH,
                "Migrated file instance not found in data modeling after migration.",
            )
        migrated_node = nodes[0]
        if migrated_node.external_id != file.external_id:
            raise AssertionError("Migrated file instance external ID does not match expected value.")
        content = client.files.download_bytes(instance_id=NodeId(space, external_id=file.external_id))
        if content != b"Toolkit classic file content for migration smoke test.":
            raise AssertionError("Migrated file content does not match expected content.")
        migrated_file = client.tool.filemetadata.retrieve([file.as_id()])
        if len(migrated_file) != 1:
            raise EndpointAssertionError(
                client.tool.filemetadata._method_endpoint_map["retrieve"].path,
                "Failed to retrieve migrated file metadata from CDF after migration.",
            )
        if migrated_file[0].instance_id is None:
            raise AssertionError("Migrated file metadata has no instance ID after migration.")
