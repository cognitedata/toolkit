import time
from collections.abc import Iterator
from pathlib import Path

import pytest
from cognite.client.data_classes import (
    DataSet,
    FileMetadata,
    ThreeDModelRevision,
    ThreeDModelRevisionWrite,
    filters,
)
from cognite.client.data_classes.data_modeling import NodeApply, NodeOrEdgeData, Space, ViewId

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.three_d import ThreeDModelClassicRequest, ThreeDModelResponse
from cognite_toolkit._cdf_tk.commands._migrate.data_mapper import ThreeDMapper
from cognite_toolkit._cdf_tk.commands._migrate.data_model import COGNITE_MIGRATION_MODEL, SPACE_SOURCE_VIEW_ID
from cognite_toolkit._cdf_tk.commands._migrate.issues import ThreeDModelMigrationIssue
from cognite_toolkit._cdf_tk.commands._migrate.migration_io import ThreeDMigrationIO
from cognite_toolkit._cdf_tk.storageio import UploadItem
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.http_client import FailedRequestMessage, FailedResponse, HTTPClient
from tests.test_integration.constants import RUN_UNIQUE_ID
from tests_smoke.exceptions import EndpointAssertionError


@pytest.fixture
def a_three_d_model(
    toolkit_client: ToolkitClient, three_d_file: FileMetadata, smoke_dataset: DataSet, smoke_space: Space
) -> Iterator[ThreeDModelResponse]:
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

    revision: ThreeDModelRevision = client.three_d.revisions.create(
        model.id, ThreeDModelRevisionWrite(file_id=three_d_file.id, published=True)
    )
    if not isinstance(revision, ThreeDModelRevision):
        raise EndpointAssertionError(
            client.three_d.revisions._RESOURCE_PATH, "Failed to create 3D model revision for migration test."
        )

    max_time = time.time() + 300  # 5 minutes timeout
    while revision.status in {"Processing", "Queued"}:
        revision = client.three_d.revisions.retrieve(model.id, revision.id)
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
        self, a_three_d_model: ThreeDModelResponse, toolkit_client: ToolkitClient, tmp_path: Path, smoke_space: Space
    ) -> None:
        client = toolkit_client
        model = a_three_d_model

        mapper = ThreeDMapper(client)

        mapped = mapper.map([model])
        if len(mapped) != 1:
            raise AssertionError(f"{self.ERROR_HEADING}Failed to map classic 3D to data modeling format.")
        migration_request, issue = mapped[0]
        if not isinstance(issue, ThreeDModelMigrationIssue):
            raise AssertionError(f"{self.ERROR_HEADING}Issue object not of expected type got {type(issue)}.")
        if issue.has_issues:
            raise AssertionError(f"{self.ERROR_HEADING}Issues: {humanize_collection(issue.error_message)}")

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
