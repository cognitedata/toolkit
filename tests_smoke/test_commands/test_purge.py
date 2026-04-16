import contextlib
import time
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import TypeVar

import pytest
from cognite.client import data_modeling as dm
from cognite.client.data_classes import (
    Asset,
    AssetWrite,
    DataSet,
    DataSetWrite,
    Event,
    EventWrite,
    ExtractionPipeline,
    ExtractionPipelineWrite,
    FileMetadata,
    FileMetadataWrite,
    LabelDefinition,
    LabelDefinitionWrite,
    Relationship,
    RelationshipWrite,
    Sequence,
    SequenceColumnWrite,
    SequenceWrite,
    ThreeDModel,
    ThreeDModelWrite,
    TimeSeries,
    TimeSeriesWrite,
    Transformation,
    TransformationWrite,
    Workflow,
    WorkflowUpsert,
)
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply, CogniteTimeSeriesApply
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils import datetime_to_ms

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.identifiers import InternalId, NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import SpaceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.pending_instance_id import PendingInstanceId
from cognite_toolkit._cdf_tk.commands import PurgeCommand
from cognite_toolkit._cdf_tk.storageio.selectors import InstanceFileSelector
from tests.test_integration.constants import RUN_UNIQUE_ID

T = TypeVar("T")


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
) -> dm.NodeList:
    """Retry DM instances retrieve until it returns 0 nodes or timeout expires."""
    start_time = time.monotonic()
    result = client.data_modeling.instances.retrieve(node_ids)
    while len(result.nodes) != 0 and (time.monotonic() - start_time) < timeout_seconds:
        time.sleep(poll_interval)
        result = client.data_modeling.instances.retrieve(node_ids)
    return result.nodes


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
    file = CogniteFileApply(
        space=space,
        external_id=f"test_file_purge_with_unlink_{RUN_UNIQUE_ID}",
        name="Test File for Purge with Unlink",
        mime_type="text/plain",
    )
    ts = CogniteTimeSeriesApply(
        space=space,
        external_id=f"test_ts_purge_with_unlink_{RUN_UNIQUE_ID}",
        name="Test TS for Purge with Unlink",
        is_step=False,
        time_series_type="numeric",
    )
    classic_file = FileMetadataWrite(
        name="Test File for Purge with Unlink",
        external_id=file.external_id,
        mime_type=file.mime_type,
    )
    classic_ts = TimeSeriesWrite(
        external_id=ts.external_id,
        name=ts.name,
        is_step=ts.is_step,
        is_string=ts.time_series_type == "string",
    )
    file_id: int | None = None
    ts_id: int | None = None
    try:
        client.data_modeling.instances.delete([file.as_id(), ts.as_id()])
        client.files.delete(external_id=classic_file.external_id, ignore_unknown_ids=True)
        client.time_series.delete(external_id=classic_ts.external_id, ignore_unknown_ids=True)

        created_file = client.files.upload_bytes(b"Sample file content", **classic_file.dump(camel_case=False))
        file_id = created_file.id
        created_ts = client.time_series.create(classic_ts)
        ts_id = created_ts.id
        ts_ms = datetime_to_ms(datetime(2020, 1, 1, 0, 0, 0))
        client.time_series.data.insert(datapoints=[(ts_ms, 1.0)], id=ts_id)

        client.tool.filemetadata.set_pending_ids(
            [
                PendingInstanceId(
                    pending_instance_id=NodeId(space=file.space, external_id=file.external_id),
                    id=file_id,
                )
            ]
        )
        client.tool.timeseries.set_pending_ids(
            [
                PendingInstanceId(
                    pending_instance_id=NodeId(space=ts.space, external_id=ts.external_id),
                    id=ts_id,
                )
            ]
        )

        created = client.data_modeling.instances.apply([file, ts])
        if len(created.nodes) != 2:
            raise AssertionError(
                f"Expected 2 data modeling nodes after apply, got {len(created.nodes)} nodes: {created.nodes!r}"
            )

        yield (file.as_id(), file_id), (ts.as_id(), ts_id)
    finally:
        client.data_modeling.instances.delete([file.as_id(), ts.as_id()])
        if file_id is not None:
            client.tool.filemetadata.unlink_instance_ids([InternalId(id=file_id)])
            client.files.delete(id=file_id, ignore_unknown_ids=True)
        if ts_id is not None:
            client.tool.timeseries.unlink_instance_ids([InternalId(id=ts_id)])
            client.time_series.delete(id=ts_id, ignore_unknown_ids=True)


@dataclass
class PopulatedDataSet:
    dataset: DataSet
    asset: Asset
    event: Event
    sequence: Sequence
    timeseries: TimeSeries
    file: FileMetadata
    label: LabelDefinition
    relationships: Relationship
    three_d: ThreeDModel
    workflow: Workflow
    transformation: Transformation
    extraction_pipeline: ExtractionPipeline


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
    dataset = DataSetWrite(name=name, external_id=external_id)
    created = client.data_sets.retrieve(external_id=dataset.external_id)
    if not created:
        created = client.data_sets.create(dataset)

    asset = AssetWrite(
        name="Test Asset",
        external_id=f"test_asset_{RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
    )
    created_asset = client.assets.create(asset)

    event = EventWrite(
        external_id=f"test_event_{RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
    )
    created_event = client.events.create(event)

    sequence = SequenceWrite(
        external_id=f"test_sequence_{RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
        columns=[SequenceColumnWrite(external_id="col1", value_type="String")],
    )
    created_sequence = client.sequences.create(sequence)

    timeseries = TimeSeriesWrite(
        external_id=f"test_timeseries_{RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
    )
    created_timeseries = client.time_series.create(timeseries)

    file = FileMetadataWrite(
        name="Test File",
        external_id=f"test_file_{RUN_UNIQUE_ID}_{no}",
        mime_type="text/plain",
        data_set_id=created.id,
    )
    created_file, _ = client.files.create(file)

    label = LabelDefinitionWrite(
        name="Test Label",
        external_id=f"test_label_{RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
    )
    created_label = client.labels.create(label)

    asset_external_id = created_asset.external_id
    event_external_id = created_event.external_id
    if asset_external_id is None or event_external_id is None:
        raise AssertionError("Expected external_id on created asset and event for relationship setup")

    relationship = RelationshipWrite(
        external_id=f"test_relationship_{RUN_UNIQUE_ID}_{no}",
        source_external_id=asset_external_id,
        target_external_id=event_external_id,
        source_type="asset",
        target_type="event",
        data_set_id=created.id,
    )
    created_relationship = client.relationships.create(relationship)

    three_d = ThreeDModelWrite(
        name=f"Test 3D Model {RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
    )
    three_d_created = client.three_d.models.create(three_d)
    if isinstance(three_d_created, list):
        if not three_d_created:
            raise AssertionError("Expected at least one 3D model from create")
        created_three_d = three_d_created[0]
    else:
        created_three_d = three_d_created

    workflow = WorkflowUpsert(
        external_id=f"test_workflow_{RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
    )
    created_workflow = client.workflows.upsert(workflow)

    transformation = TransformationWrite(
        name="Test Transformation",
        external_id=f"test_transformation_{RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
        ignore_null_fields=True,
    )
    created_transformation = client.transformations.create(transformation)

    extraction_pipeline = ExtractionPipelineWrite(
        name="Test Extraction Pipeline",
        external_id=f"test_extraction_pipeline_{RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
    )
    created_extraction_pipeline = client.extraction_pipelines.create(extraction_pipeline)

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
    client.assets.delete(id=populated.asset.id, ignore_unknown_ids=True)
    client.events.delete(id=populated.event.id, ignore_unknown_ids=True)
    client.sequences.delete(id=populated.sequence.id, ignore_unknown_ids=True)
    client.time_series.delete(id=populated.timeseries.id, ignore_unknown_ids=True)
    client.files.delete(id=populated.file.id, ignore_unknown_ids=True)
    client.labels.delete(external_id=populated.label.external_id)
    rel_external_id = populated.relationships.external_id
    if rel_external_id is not None:
        client.relationships.delete(external_id=rel_external_id, ignore_unknown_ids=True)
    with contextlib.suppress(CogniteAPIError):
        client.three_d.models.delete(id=populated.three_d.id)
    client.workflows.delete(external_id=populated.workflow.external_id, ignore_unknown_ids=True)
    client.transformations.delete(id=populated.transformation.id, ignore_unknown_ids=True)
    with contextlib.suppress(CogniteNotFoundError):
        client.extraction_pipelines.delete(id=populated.extraction_pipeline.id)


class TestPurgeSmoke:
    def test_purge_instances_with_unlink(
        self,
        file_ts_nodes: tuple[tuple[dm.NodeId, int], tuple[dm.NodeId, int]],
        toolkit_client: ToolkitClient,
        tmp_path: Path,
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

        purge = PurgeCommand(silent=True)

        results = purge.instances(
            client,
            InstanceFileSelector(datafile=csv_path),
            dry_run=False,
            unlink=True,
            verbose=False,
            auto_yes=True,
        )
        if results.deleted != 2:
            raise AssertionError(f"Expected 2 deleted instances from purge, got {results.deleted!r}")

        retrieve_results = wait_until_dm_nodes_deleted(client, [file_node, ts_node])
        if len(retrieve_results) != 0:
            raise AssertionError(
                f"Instances were not purged; expected 0 nodes, got {len(retrieve_results)}: {retrieve_results!r}"
            )

        classic_file = wait_until_exists(lambda: client.files.retrieve(id=file_id))
        if classic_file is None:
            raise AssertionError("Classic file was not found after purge; expected it to remain unlinked")

        classic_ts = wait_until_exists(lambda: client.time_series.retrieve(id=ts_id))
        if classic_ts is None:
            raise AssertionError("Classic time series was not found after purge; expected it to remain unlinked")

    def test_purge_dataset_include_data(
        self, toolkit_client: ToolkitClient, populated_dataset: PopulatedDataSet
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
        )
        if wait_until_deleted(lambda: client.assets.retrieve(external_id=populated.asset.external_id)) is not None:
            raise AssertionError("Expected asset to be deleted when include_data=True")
        if wait_until_deleted(lambda: client.events.retrieve(external_id=populated.event.external_id)) is not None:
            raise AssertionError("Expected event to be deleted when include_data=True")
        if (
            wait_until_deleted(lambda: client.sequences.retrieve(external_id=populated.sequence.external_id))
            is not None
        ):
            raise AssertionError("Expected sequence to be deleted when include_data=True")
        if (
            wait_until_deleted(lambda: client.time_series.retrieve(external_id=populated.timeseries.external_id))
            is not None
        ):
            raise AssertionError("Expected time series to be deleted when include_data=True")
        if wait_until_deleted(lambda: client.files.retrieve(external_id=populated.file.external_id)) is not None:
            raise AssertionError("Expected file to be deleted when include_data=True")

        labels_for_dataset = wait_until_list_empty(
            lambda: list(client.labels.list(data_set_external_ids=dataset_external_id))
        )
        if len(labels_for_dataset) != 0:
            raise AssertionError(
                f"Expected no labels listed under dataset after purge; got {len(labels_for_dataset)} labels"
            )
        relationships = wait_until_list_empty(
            lambda: list(client.relationships.list(source_external_ids=[populated.asset.external_id]))
        )
        if len(relationships) != 0:
            raise AssertionError(
                f"Expected relationships involving purged asset to be gone; got {len(relationships)} relationships"
            )
        if wait_until_deleted(lambda: client.three_d.models.retrieve(id=populated.three_d.id)) is not None:
            raise AssertionError("Expected 3D model to be deleted when include_data=True")

        workflow = wait_until_exists(
            lambda: client.workflows.retrieve(external_id=populated.workflow.external_id, ignore_unknown_ids=True)
        )
        if workflow is None:
            raise AssertionError("Expected workflow to remain when include_configurations=False")
        if (
            wait_until_exists(lambda: client.transformations.retrieve(external_id=populated.transformation.external_id))
            is None
        ):
            raise AssertionError("Expected transformation to remain when include_configurations=False")
        if (
            wait_until_exists(
                lambda: client.extraction_pipelines.retrieve(external_id=populated.extraction_pipeline.external_id)
            )
            is None
        ):
            raise AssertionError("Expected extraction pipeline to remain when include_configurations=False")

    def test_purge_dataset_include_configurations(
        self, toolkit_client: ToolkitClient, populated_datasets_2: PopulatedDataSet
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
        )
        if wait_until_exists(lambda: client.assets.retrieve(external_id=populated.asset.external_id)) is None:
            raise AssertionError("Expected asset to remain when include_data=False")
        if wait_until_exists(lambda: client.events.retrieve(external_id=populated.event.external_id)) is None:
            raise AssertionError("Expected event to remain when include_data=False")
        if wait_until_exists(lambda: client.sequences.retrieve(external_id=populated.sequence.external_id)) is None:
            raise AssertionError("Expected sequence to remain when include_data=False")
        if wait_until_exists(lambda: client.time_series.retrieve(external_id=populated.timeseries.external_id)) is None:
            raise AssertionError("Expected time series to remain when include_data=False")
        if wait_until_exists(lambda: client.files.retrieve(external_id=populated.file.external_id)) is None:
            raise AssertionError("Expected file to remain when include_data=False")

        labels_for_dataset = client.labels.list(data_set_external_ids=dataset_external_id)
        if len(labels_for_dataset) < 1:
            raise AssertionError(
                f"Expected at least one label still associated with dataset listing; got {len(labels_for_dataset)}"
            )
        relationships = client.relationships.list(source_external_ids=[populated.asset.external_id])
        if len(relationships) != 1:
            raise AssertionError(
                f"Expected one relationship when data retained; got {len(relationships)} relationships"
            )
        if wait_until_exists(lambda: client.three_d.models.retrieve(id=populated.three_d.id)) is None:
            raise AssertionError("Expected 3D model to remain when include_data=False")

        if (
            wait_until_deleted(
                lambda: client.workflows.retrieve(external_id=populated.workflow.external_id, ignore_unknown_ids=True)
            )
            is not None
        ):
            raise AssertionError("Expected workflow to be deleted when include_configurations=True")
        if (
            wait_until_deleted(
                lambda: client.transformations.retrieve(external_id=populated.transformation.external_id)
            )
            is not None
        ):
            raise AssertionError("Expected transformation to be deleted when include_configurations=True")
        if (
            wait_until_deleted(
                lambda: client.extraction_pipelines.retrieve(external_id=populated.extraction_pipeline.external_id)
            )
            is not None
        ):
            raise AssertionError("Expected extraction pipeline to be deleted when include_configurations=True")
