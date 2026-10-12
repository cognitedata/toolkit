import contextlib
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TypeVar
from unittest.mock import MagicMock

import pytest
from cognite.client import data_modeling as dm
from cognite.client.utils import datetime_to_ms

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.request_classes.filters import ClassicFilter
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceResponse,
    InstanceSource,
    NodeRequest,
    SpaceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.datapoints import Datapoint, DatapointsRequest
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.extraction_pipeline import (
    ExtractionPipelineRequest,
    ExtractionPipelineResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.label import LabelRequest, LabelResponse
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from cognite_toolkit._cdf_tk.client.resource_classes.relationship import RelationshipRequest, RelationshipResponse
from cognite_toolkit._cdf_tk.client.resource_classes.sequence import (
    SequenceColumnRequest,
    SequenceRequest,
    SequenceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.three_d import (
    ThreeDModelClassicRequest,
    ThreeDModelClassicResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationRequest, TransformationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowRequest, WorkflowResponse
from cognite_toolkit._cdf_tk.commands import PurgeCommand
from cognite_toolkit._cdf_tk.dataio.selectors import InstanceFileSelector
from tests.test_integration.constants import RUN_UNIQUE_ID

T = TypeVar("T")


def _first(items: list[T]) -> T | None:
    return items[0] if items else None


def _external_id(external_id: str | None) -> ExternalId:
    if external_id is None:
        raise AssertionError("Expected an external id")
    return ExternalId(external_id=external_id)


def _labels_in_dataset(client: ToolkitClient, dataset_external_id: str) -> list[LabelResponse]:
    return [
        label
        for page in client.tool.labels.iterate(
            filter=ClassicFilter(data_set_ids=[ExternalId(external_id=dataset_external_id)]),
            limit=None,
        )
        for label in page
    ]


def _relationships_from_source(client: ToolkitClient, source_external_id: str | None) -> list[RelationshipResponse]:
    if source_external_id is None:
        return []
    return [
        item for item in client.tool.relationships.list(limit=None) if item.source_external_id == source_external_id
    ]


def _retrieve_3d_model(client: ToolkitClient, model_id: int) -> ThreeDModelClassicResponse | None:
    try:
        return _first(client.tool.three_d.models_classic.retrieve([InternalId(id=model_id)]))
    except ToolkitAPIError:
        return None


def wait_until_deleted(
    retrieve_func: Callable[[], T | None],
    timeout_seconds: float = 30.0,
    poll_interval: float = 1.0,
) -> T | None:
    """Retry retrieve until it returns None (deleted) or timeout expires.

    Returns the last retrieved value (should be None if deletion was successful).
    """
    start_time = time.monotonic()
    result = retrieve_func()
    while result is not None and (time.monotonic() - start_time) < timeout_seconds:
        time.sleep(poll_interval)
        result = retrieve_func()
    return result


def wait_until_exists(
    retrieve_func: Callable[[], T | None],
    timeout_seconds: float = 30.0,
    poll_interval: float = 1.0,
) -> T | None:
    """Retry retrieve until it returns a value (exists) or timeout expires.

    Returns the retrieved value (should be not None if exists).
    """
    start_time = time.monotonic()
    result = retrieve_func()
    while result is None and (time.monotonic() - start_time) < timeout_seconds:
        time.sleep(poll_interval)
        result = retrieve_func()
    return result


def wait_until_dm_nodes_deleted(
    client: ToolkitClient,
    node_ids: list[dm.NodeId],
    timeout_seconds: float = 30.0,
    poll_interval: float = 1.0,
) -> list[InstanceResponse]:
    """Retry DM instances retrieve until it returns 0 nodes or timeout expires."""
    ids = [NodeId(space=node.space, external_id=node.external_id) for node in node_ids]
    start_time = time.monotonic()
    result = client.tool.instances.retrieve(ids)
    while len(result) != 0 and (time.monotonic() - start_time) < timeout_seconds:
        time.sleep(poll_interval)
        result = client.tool.instances.retrieve(ids)
    return result


def wait_until_list_empty(
    list_func: Callable[[], list[T]],
    timeout_seconds: float = 30.0,
    poll_interval: float = 1.0,
) -> list[T]:
    """Retry list until it returns empty or timeout expires."""
    start_time = time.monotonic()
    result = list_func()
    while len(result) != 0 and (time.monotonic() - start_time) < timeout_seconds:
        time.sleep(poll_interval)
        result = list_func()
    return result


@pytest.fixture()
def file_ts_nodes(
    toolkit_client: ToolkitClient, smoke_space: SpaceResponse
) -> Iterable[tuple[tuple[dm.NodeId, int], tuple[dm.NodeId, int]]]:
    client = toolkit_client
    space = smoke_space.space
    file_external_id = f"test_file_purge_with_unlink_{RUN_UNIQUE_ID}"
    file_mime_type = "text/plain"

    file_node = NodeRequest(
        space=space,
        external_id=file_external_id,
        sources=[
            InstanceSource(
                source=ViewId(space="cdf_cdm", external_id="CogniteFile", version="v1"),
                properties={"name": "Test File for Purge with Unlink", "mimeType": file_mime_type},
            )
        ],
    )
    ts_is_step = False
    ts_type = "numeric"
    ts_node = NodeRequest(
        space=space,
        external_id=f"test_ts_purge_with_unlink_{RUN_UNIQUE_ID}",
        sources=[
            InstanceSource(
                source=ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1"),
                properties={"name": "Test TS for Purge with Unlink", "isStep": ts_is_step, "type": ts_type},
            )
        ],
    )
    classic_file = FileMetadataRequest(
        name="Test File for Purge with Unlink",
        external_id=file_external_id,
        mime_type=file_mime_type,
    )
    classic_ts = TimeSeriesRequest(
        external_id=ts_node.external_id,
        name="Test TS for Purge with Unlink",
        is_step=ts_is_step,
        is_string=ts_type == "string",
    )
    file_id: int | None = None
    ts_id: int | None = None
    try:
        client.tool.instances.delete([file_node.as_id(), ts_node.as_id()])
        client.tool.filemetadata.delete([classic_file.as_id()], ignore_unknown_ids=True)
        client.tool.timeseries.delete([classic_ts.as_id()], ignore_unknown_ids=True)

        created_files = client.tool.filemetadata.create([classic_file])
        created_file = created_files[0]
        if created_file.upload_url is not None:
            client.tool.filemetadata.upload_file(
                b"Sample file content", created_file.upload_url, created_file.mime_type
            )
        file_id = created_file.id
        created_ts = client.tool.timeseries.create([classic_ts])[0]
        ts_id = created_ts.id
        ts_ms = datetime_to_ms(datetime(2020, 1, 1, 0, 0, 0))
        client.tool.timeseries.datapoints.create(
            [DatapointsRequest(id=ts_id, datapoints=[Datapoint(timestamp=ts_ms, value=1.0)])]
        )

        client.tool.filemetadata.set_pending_ids([PendingInstanceId(pending_instance_id=file_node.as_id(), id=file_id)])
        client.tool.timeseries.set_pending_ids(
            [
                PendingInstanceId(
                    pending_instance_id=ts_node.as_id(),
                    id=ts_id,
                )
            ]
        )

        created = client.tool.instances.create([file_node, ts_node])
        if len(created) != 2:
            raise AssertionError(f"Expected 2 data modeling nodes after apply, got {len(created)} nodes: {created!r}")

        yield (file_node.as_id(), created_file.id), (ts_node.as_id(), created_ts.id)
    finally:
        client.tool.instances.delete([file_node.as_id(), ts_node.as_id()])
        if file_id is not None:
            client.tool.filemetadata.unlink_instance_ids([InternalId(id=file_id)])
            client.tool.filemetadata.delete([InternalId(id=file_id)], ignore_unknown_ids=True)
        if ts_id is not None:
            client.tool.timeseries.unlink_instance_ids([InternalId(id=ts_id)])
            client.tool.timeseries.delete([InternalId(id=ts_id)], ignore_unknown_ids=True)


@dataclass
class PopulatedDataSet:
    dataset: DataSetResponse
    asset: AssetResponse
    event: EventResponse
    sequence: SequenceResponse
    timeseries: TimeSeriesResponse
    file: FileMetadataResponse
    label: LabelResponse
    relationships: RelationshipResponse
    three_d: ThreeDModelClassicResponse
    workflow: WorkflowResponse
    transformation: TransformationResponse
    extraction_pipeline: ExtractionPipelineResponse


@pytest.fixture()
def populated_dataset(toolkit_client: ToolkitClient) -> Iterable[PopulatedDataSet]:
    populated = create_populated_dataset(
        toolkit_client, name="toolkit_test_purge_dataset", external_id="toolkit_test_purge_dataset", no=1
    )
    yield populated
    cleanup_populated_dataset(toolkit_client, populated)


@pytest.fixture()
def populated_datasets_2(toolkit_client: ToolkitClient) -> Iterable[PopulatedDataSet]:
    populated2 = create_populated_dataset(
        toolkit_client, name="toolkit_test_purge_dataset_2", external_id="toolkit_test_purge_dataset_2", no=2
    )
    yield populated2
    cleanup_populated_dataset(toolkit_client, populated2)


def create_populated_dataset(toolkit_client: ToolkitClient, name: str, external_id: str, no: int) -> PopulatedDataSet:
    client = toolkit_client
    dataset = DataSetRequest(name=name, external_id=external_id)
    retrieved = client.tool.datasets.retrieve([dataset.as_id()], ignore_unknown_ids=True)
    created = retrieved[0] if retrieved else client.tool.datasets.create([dataset])[0]

    created_asset = client.tool.assets.create(
        [
            AssetRequest(
                name="Test Asset",
                external_id=f"test_asset_{RUN_UNIQUE_ID}_{no}",
                data_set_id=created.id,
            )
        ]
    )[0]
    created_event = client.tool.events.create(
        [EventRequest(external_id=f"test_event_{RUN_UNIQUE_ID}_{no}", data_set_id=created.id)]
    )[0]
    created_sequence = client.tool.sequences.create(
        [
            SequenceRequest(
                external_id=f"test_sequence_{RUN_UNIQUE_ID}_{no}",
                data_set_id=created.id,
                columns=[SequenceColumnRequest(external_id="col1", value_type="STRING")],
            )
        ]
    )[0]
    created_timeseries = client.tool.timeseries.create(
        [TimeSeriesRequest(external_id=f"test_timeseries_{RUN_UNIQUE_ID}_{no}", data_set_id=created.id)]
    )[0]
    created_file = client.tool.filemetadata.create(
        [
            FileMetadataRequest(
                name="Test File",
                external_id=f"test_file_{RUN_UNIQUE_ID}_{no}",
                mime_type="text/plain",
                data_set_id=created.id,
            )
        ]
    )[0]
    created_label = client.tool.labels.create(
        [
            LabelRequest(
                name="Test Label",
                external_id=f"test_label_{RUN_UNIQUE_ID}_{no}",
                data_set_id=created.id,
            )
        ]
    )[0]

    asset_external_id = created_asset.external_id
    event_external_id = created_event.external_id
    if asset_external_id is None or event_external_id is None:
        raise AssertionError("Expected external_id on created asset and event for relationship setup")

    created_relationship = client.tool.relationships.create(
        [
            RelationshipRequest(
                external_id=f"test_relationship_{RUN_UNIQUE_ID}_{no}",
                source_external_id=asset_external_id,
                source_type="asset",
                target_external_id=event_external_id,
                target_type="event",
                data_set_id=created.id,
            )
        ]
    )[0]
    created_three_d = client.tool.three_d.models_classic.create(
        [
            ThreeDModelClassicRequest(
                name=f"Test 3D Model {RUN_UNIQUE_ID}_{no}",
                data_set_id=created.id,
            )
        ]
    )[0]
    created_workflow = client.tool.workflows.create(
        [WorkflowRequest(external_id=f"test_workflow_{RUN_UNIQUE_ID}_{no}", data_set_id=created.id)]
    )[0]
    created_transformation = client.tool.transformations.create(
        [
            TransformationRequest(
                name="Test Transformation",
                external_id=f"test_transformation_{RUN_UNIQUE_ID}_{no}",
                data_set_id=created.id,
                ignore_null_fields=True,
            )
        ]
    )[0]
    created_extraction_pipeline = client.tool.extraction_pipelines.create(
        [
            ExtractionPipelineRequest(
                name="Test Extraction Pipeline",
                external_id=f"test_extraction_pipeline_{RUN_UNIQUE_ID}_{no}",
                data_set_id=created.id,
            )
        ]
    )[0]

    return PopulatedDataSet(
        dataset=created,
        asset=created_asset,
        event=created_event,
        sequence=created_sequence,
        timeseries=created_timeseries,
        file=created_file,
        label=created_label,
        relationships=created_relationship,
        three_d=created_three_d,
        workflow=created_workflow,
        transformation=created_transformation,
        extraction_pipeline=created_extraction_pipeline,
    )


def cleanup_populated_dataset(client: ToolkitClient, populated: PopulatedDataSet) -> None:
    client.tool.assets.delete([InternalId(id=populated.asset.id)], ignore_unknown_ids=True)
    client.tool.events.delete([InternalId(id=populated.event.id)], ignore_unknown_ids=True)
    client.tool.sequences.delete([InternalId(id=populated.sequence.id)], ignore_unknown_ids=True)
    client.tool.timeseries.delete([InternalId(id=populated.timeseries.id)], ignore_unknown_ids=True)
    client.tool.filemetadata.delete([InternalId(id=populated.file.id)], ignore_unknown_ids=True)
    client.tool.labels.delete([populated.label.as_id()])
    client.tool.relationships.delete(
        [ExternalId(external_id=populated.relationships.external_id)], ignore_unknown_ids=True
    )
    with contextlib.suppress(ToolkitAPIError):
        client.tool.three_d.models_classic.delete([InternalId(id=populated.three_d.id)])
    with contextlib.suppress(ToolkitAPIError):
        client.tool.workflows.delete([populated.workflow.as_id()])
    client.tool.transformations.delete([InternalId(id=populated.transformation.id)], ignore_unknown_ids=True)
    client.tool.extraction_pipelines.delete([InternalId(id=populated.extraction_pipeline.id)], ignore_unknown_ids=True)


class TestPurgeSmoke:
    def test_purge_instances_with_unlink(
        self,
        file_ts_nodes: tuple[tuple[dm.NodeId, int], tuple[dm.NodeId, int]],
        toolkit_client: ToolkitClient,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        client = toolkit_client
        (file_node, file_id), (ts_node, ts_id) = file_ts_nodes

        csv_path = tmp_path / "test.csv"
        csv_path.write_text(
            f"""space,externalId,instanceType
{file_node.space},{file_node.external_id},node
{ts_node.space},{ts_node.external_id},node
""",
            encoding="utf-8",
        )

        mock_questionary = MagicMock()
        mock_questionary.confirm.return_value.ask.return_value = True
        mock_questionary.text.return_value.unsafe_ask.return_value = client.config.project
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands._purge.questionary", mock_questionary)
        monkeypatch.setattr("cognite_toolkit._cdf_tk.commands._utils.questionary", mock_questionary)

        purge = PurgeCommand(silent=True)

        results = purge.instances(
            client,
            InstanceFileSelector(datafile=csv_path),
            dry_run=False,
            unlink=True,
            verbose=False,
            log_dir=tmp_path / "log",
        )
        if results.deleted != 2:
            raise AssertionError(f"Expected 2 deleted instances from purge, got {results.deleted!r}")

        retrieve_results = wait_until_dm_nodes_deleted(client, [file_node, ts_node])
        if len(retrieve_results) != 0:
            raise AssertionError(
                f"Instances were not purged; expected 0 nodes, got {len(retrieve_results)}: {retrieve_results!r}"
            )

        classic_file = wait_until_exists(
            lambda: _first(client.tool.filemetadata.retrieve([InternalId(id=file_id)], ignore_unknown_ids=True))
        )
        if classic_file is None:
            raise AssertionError("Classic file was not found after purge; expected it to remain unlinked")

        classic_ts = wait_until_exists(
            lambda: _first(client.tool.timeseries.retrieve([InternalId(id=ts_id)], ignore_unknown_ids=True))
        )
        if classic_ts is None:
            raise AssertionError("Classic time series was not found after purge; expected it to remain unlinked")

    def test_purge_dataset_include_data(
        self,
        toolkit_client: ToolkitClient,
        populated_dataset: PopulatedDataSet,
        tmp_path: Path,
    ) -> None:
        client = toolkit_client
        populated = populated_dataset
        purge = PurgeCommand(silent=True)
        dataset_external_id = populated.dataset.external_id
        if dataset_external_id is None:
            raise AssertionError("Populated dataset is missing external_id")

        _ = purge.dataset(
            client,
            selected_data_set_external_id=dataset_external_id,
            archive_dataset=False,
            include_data=True,
            include_configurations=False,
            dry_run=False,
            auto_yes=True,
            verbose=False,
            log_dir=tmp_path / "log",
        )
        if (
            wait_until_deleted(
                lambda: _first(
                    client.tool.assets.retrieve([_external_id(populated.asset.external_id)], ignore_unknown_ids=True)
                )
            )
            is not None
        ):
            raise AssertionError("Expected asset to be deleted when include_data=True")
        if (
            wait_until_deleted(
                lambda: _first(
                    client.tool.events.retrieve([_external_id(populated.event.external_id)], ignore_unknown_ids=True)
                )
            )
            is not None
        ):
            raise AssertionError("Expected event to be deleted when include_data=True")
        if (
            wait_until_deleted(
                lambda: _first(
                    client.tool.sequences.retrieve(
                        [_external_id(populated.sequence.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is not None
        ):
            raise AssertionError("Expected sequence to be deleted when include_data=True")
        if (
            wait_until_deleted(
                lambda: _first(
                    client.tool.timeseries.retrieve(
                        [_external_id(populated.timeseries.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is not None
        ):
            raise AssertionError("Expected time series to be deleted when include_data=True")
        if (
            wait_until_deleted(
                lambda: _first(
                    client.tool.filemetadata.retrieve(
                        [_external_id(populated.file.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is not None
        ):
            raise AssertionError("Expected file to be deleted when include_data=True")

        labels_for_dataset = wait_until_list_empty(lambda: _labels_in_dataset(client, dataset_external_id))
        if len(labels_for_dataset) != 0:
            raise AssertionError(
                f"Expected no labels listed under dataset after purge; got {len(labels_for_dataset)} labels"
            )
        relationships = wait_until_list_empty(lambda: _relationships_from_source(client, populated.asset.external_id))
        if len(relationships) != 0:
            raise AssertionError(
                f"Expected relationships involving purged asset to be gone; got {len(relationships)} relationships"
            )
        if wait_until_deleted(lambda: _retrieve_3d_model(client, populated.three_d.id)) is not None:
            raise AssertionError("Expected 3D model to be deleted when include_data=True")

        workflow = wait_until_exists(
            lambda: _first(client.tool.workflows.retrieve([populated.workflow.as_id()], ignore_unknown_ids=True))
        )
        if workflow is None:
            raise AssertionError("Expected workflow to remain when include_configurations=False")
        if (
            wait_until_exists(
                lambda: _first(
                    client.tool.transformations.retrieve(
                        [_external_id(populated.transformation.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is None
        ):
            raise AssertionError("Expected transformation to remain when include_configurations=False")
        if (
            wait_until_exists(
                lambda: _first(
                    client.tool.extraction_pipelines.retrieve(
                        [_external_id(populated.extraction_pipeline.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is None
        ):
            raise AssertionError("Expected extraction pipeline to remain when include_configurations=False")

    def test_purge_dataset_include_configurations(
        self, toolkit_client: ToolkitClient, populated_datasets_2: PopulatedDataSet, tmp_path: Path
    ) -> None:
        client = toolkit_client
        populated = populated_datasets_2
        purge = PurgeCommand(silent=True)
        dataset_external_id = populated.dataset.external_id
        if dataset_external_id is None:
            raise AssertionError("Populated dataset is missing external_id")

        _ = purge.dataset(
            client,
            selected_data_set_external_id=dataset_external_id,
            archive_dataset=False,
            include_data=False,
            include_configurations=True,
            dry_run=False,
            auto_yes=True,
            verbose=False,
            log_dir=tmp_path / "log",
        )
        if (
            wait_until_exists(
                lambda: _first(
                    client.tool.assets.retrieve([_external_id(populated.asset.external_id)], ignore_unknown_ids=True)
                )
            )
            is None
        ):
            raise AssertionError("Expected asset to remain when include_data=False")
        if (
            wait_until_exists(
                lambda: _first(
                    client.tool.events.retrieve([_external_id(populated.event.external_id)], ignore_unknown_ids=True)
                )
            )
            is None
        ):
            raise AssertionError("Expected event to remain when include_data=False")
        if (
            wait_until_exists(
                lambda: _first(
                    client.tool.sequences.retrieve(
                        [_external_id(populated.sequence.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is None
        ):
            raise AssertionError("Expected sequence to remain when include_data=False")
        if (
            wait_until_exists(
                lambda: _first(
                    client.tool.timeseries.retrieve(
                        [_external_id(populated.timeseries.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is None
        ):
            raise AssertionError("Expected time series to remain when include_data=False")
        if (
            wait_until_exists(
                lambda: _first(
                    client.tool.filemetadata.retrieve(
                        [_external_id(populated.file.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is None
        ):
            raise AssertionError("Expected file to remain when include_data=False")

        labels_for_dataset = _labels_in_dataset(client, dataset_external_id)
        if len(labels_for_dataset) < 1:
            raise AssertionError(
                f"Expected at least one label still associated with dataset listing; got {len(labels_for_dataset)}"
            )
        relationships = _relationships_from_source(client, populated.asset.external_id)
        if len(relationships) != 1:
            raise AssertionError(
                f"Expected one relationship when data retained; got {len(relationships)} relationships"
            )
        if wait_until_exists(lambda: _retrieve_3d_model(client, populated.three_d.id)) is None:
            raise AssertionError("Expected 3D model to remain when include_data=False")

        if (
            wait_until_deleted(
                lambda: _first(client.tool.workflows.retrieve([populated.workflow.as_id()], ignore_unknown_ids=True))
            )
            is not None
        ):
            raise AssertionError("Expected workflow to be deleted when include_configurations=True")
        if (
            wait_until_deleted(
                lambda: _first(
                    client.tool.transformations.retrieve(
                        [_external_id(populated.transformation.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is not None
        ):
            raise AssertionError("Expected transformation to be deleted when include_configurations=True")
        if (
            wait_until_deleted(
                lambda: _first(
                    client.tool.extraction_pipelines.retrieve(
                        [_external_id(populated.extraction_pipeline.external_id)], ignore_unknown_ids=True
                    )
                )
            )
            is not None
        ):
            raise AssertionError("Expected extraction pipeline to be deleted when include_configurations=True")
