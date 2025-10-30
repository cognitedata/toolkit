import contextlib
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import pytest
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
from cognite.client.data_classes.data_modeling import NodeId, Space
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply, CogniteTimeSeriesApply
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError
from cognite.client.utils import datetime_to_ms

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import PurgeCommand
from cognite_toolkit._cdf_tk.storageio.selectors import InstanceFileSelector
from tests.test_integration.constants import RUN_UNIQUE_ID


@pytest.fixture()
def file_ts_nodes(
    toolkit_client_with_pending_ids: ToolkitClient, toolkit_space: Space
) -> Iterable[tuple[tuple[NodeId, int], tuple[NodeId, int]]]:
    client = toolkit_client_with_pending_ids
    file = CogniteFileApply(
        space=toolkit_space.space,
        external_id=f"test_file_purge_with_unlink_{RUN_UNIQUE_ID}",
        name="Test File for Purge with Unlink",
        mime_type="text/plain",
    )
    ts = CogniteTimeSeriesApply(
        space=toolkit_space.space,
        external_id=f"test_ts_purge_with_unlink_{RUN_UNIQUE_ID}",
        name="Test TS for Purge with Unlink",
        is_step=False,
        time_series_type="numeric",
    )
    classic_file = FileMetadataWrite(
        name=file.name,
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
        # Ensure clean state
        client.data_modeling.instances.delete([file.as_id(), ts.as_id()])
        client.files.delete(external_id=classic_file.external_id, ignore_unknown_ids=True)
        client.time_series.delete(external_id=classic_ts.external_id, ignore_unknown_ids=True)

        # Create timeseries and file
        created_file = client.files.upload_bytes(b"Sample file content", **classic_file.dump(camel_case=False))
        file_id = created_file.id
        created_ts = client.time_series.create(classic_ts)
        ts_id = created_ts.id
        client.time_series.data.insert(
            datapoints=[{"timestamp": datetime_to_ms(datetime(2020, 1, 1, 0, 0, 0)), "value": 1.0}],
            id=ts_id,
        )

        # Link them.
        client.files.set_pending_ids(file.as_id(), id=file_id)
        client.time_series.set_pending_ids(ts.as_id(), id=ts_id)

        # Create Nodes in CDM
        created = client.data_modeling.instances.apply([file, ts])
        assert len(created.nodes) == 2

        yield (file.as_id(), file_id), (ts.as_id(), ts_id)
    finally:
        client.data_modeling.instances.delete([file.as_id(), ts.as_id()])
        if file_id is not None:
            client.files.unlink_instance_ids(id=file_id)
            client.files.delete(id=file_id, ignore_unknown_ids=True)
        if ts_id is not None:
            client.time_series.unlink_instance_ids(id=ts_id)
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
    client = toolkit_client
    dataset = DataSetWrite(name="toolkit_test_purge_dataset", external_id="toolkit_test_purge_dataset")
    created = client.data_sets.retrieve(external_id=dataset.external_id)
    if not created:
        # DataSet cannot be deleted, so we create it only once and reuse it
        created = client.data_sets.create(dataset)

    asset = AssetWrite(
        name="Test Asset",
        external_id=f"test_asset_{RUN_UNIQUE_ID}",
        data_set_id=created.id,
    )
    created_asset = client.assets.create(asset)

    event = EventWrite(
        external_id=f"test_event_{RUN_UNIQUE_ID}",
        data_set_id=created.id,
    )
    created_event = client.events.create(event)

    sequence = SequenceWrite(
        external_id=f"test_sequence_{RUN_UNIQUE_ID}",
        data_set_id=created.id,
        columns=[SequenceColumnWrite(external_id="col1", value_type="String")],
    )
    created_sequence = client.sequences.create(sequence)

    timeseries = TimeSeriesWrite(
        external_id=f"test_timeseries_{RUN_UNIQUE_ID}",
        data_set_id=created.id,
    )
    created_timeseries = client.time_series.create(timeseries)

    file = FileMetadataWrite(
        name="Test File",
        external_id=f"test_file_{RUN_UNIQUE_ID}",
        mime_type="text/plain",
        data_set_id=created.id,
    )
    created_file, _ = client.files.create(file)

    label = LabelDefinitionWrite(
        name="Test Label",
        external_id=f"test_label_{RUN_UNIQUE_ID}",
        data_set_id=created.id,
    )
    created_label = client.labels.create(label)

    relationship = RelationshipWrite(
        external_id=f"test_relationship_{RUN_UNIQUE_ID}",
        source_external_id=created_asset.external_id,
        target_external_id=created_event.external_id,
        source_type="asset",
        target_type="event",
        data_set_id=created.id,
    )
    created_relationship = client.relationships.create(relationship)

    three_d = ThreeDModelWrite(
        name=f"Test 3D Model {RUN_UNIQUE_ID}",
        data_set_id=created.id,
    )
    created_three_d = client.three_d.models.create(three_d)

    workflow = WorkflowUpsert(
        external_id=f"test_workflow_{RUN_UNIQUE_ID}",
        data_set_id=created.id,
    )
    created_workflow = client.workflows.upsert(workflow)

    transformation = TransformationWrite(
        name="Test Transformation",
        external_id=f"test_transformation_{RUN_UNIQUE_ID}",
        data_set_id=created.id,
        ignore_null_fields=True,
    )
    created_transformation = client.transformations.create(transformation)

    extraction_pipeline = ExtractionPipelineWrite(
        name="Test Extraction Pipeline",
        external_id=f"test_extraction_pipeline_{RUN_UNIQUE_ID}",
        data_set_id=created.id,
    )
    created_extraction_pipeline = client.extraction_pipelines.create(extraction_pipeline)

    yield PopulatedDataSet(
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

    # Cleanup
    client.assets.delete(id=created_asset.id, ignore_unknown_ids=True)
    client.events.delete(id=created_event.id, ignore_unknown_ids=True)
    client.sequences.delete(id=created_sequence.id, ignore_unknown_ids=True)
    client.time_series.delete(id=created_timeseries.id, ignore_unknown_ids=True)
    client.files.delete(id=created_file.id, ignore_unknown_ids=True)
    client.labels.delete(external_id=created_label.external_id)
    client.relationships.delete(external_id=created_relationship.external_id, ignore_unknown_ids=True)
    with contextlib.suppress(CogniteAPIError):
        client.three_d.models.delete(id=created_three_d.id)
    client.workflows.delete(external_id=created_workflow.external_id, ignore_unknown_ids=True)
    client.transformations.delete(id=created_transformation.id, ignore_unknown_ids=True)
    with contextlib.suppress(CogniteNotFoundError):
        client.extraction_pipelines.delete(id=created_extraction_pipeline.id)


class TestPurge:
    def test_purge_instances_with_unlink(
        self,
        file_ts_nodes: tuple[tuple[NodeId, int], tuple[NodeId, int]],
        toolkit_client_with_pending_ids: ToolkitClient,
        tmp_path: Path,
    ) -> None:
        client = toolkit_client_with_pending_ids
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
            InstanceFileSelector(datafile=csv_path, validate=True),
            dry_run=False,
            unlink=True,
            verbose=False,
            auto_yes=True,
        )
        assert results.deleted == 2

        results = client.data_modeling.instances.retrieve([file_node, ts_node])
        assert len(results.nodes) == 0, "Instances were not purged"

        classic_file = client.files.retrieve(id=file_id)
        assert classic_file is not None, "File was not unlinked"

        classic_ts = client.time_series.retrieve(id=ts_id)
        assert classic_ts is not None, "Time series was not unlinked"

    def test_purge_dataset(self, toolkit_client: ToolkitClient, populated_dataset: PopulatedDataSet) -> None:
        client = toolkit_client
        populated = populated_dataset
        purge = PurgeCommand(silent=True)

        _ = purge.dataset_v2(
            client,
            selected_data_set_external_id=populated.dataset.external_id,
            archive_dataset=False,
            include_data=True,
            include_configurations=True,
            dry_run=False,
            auto_yes=True,
            verbose=False,
        )
        # Verify all items are deleted
        assert client.assets.retrieve(external_id=populated.asset.external_id) is None
        assert client.events.retrieve(external_id=populated.event.external_id) is None
        assert client.sequences.retrieve(external_id=populated.sequence.external_id) is None
        assert client.time_series.retrieve(external_id=populated.timeseries.external_id) is None
        assert client.files.retrieve(external_id=populated.file.external_id) is None
        # Labels are not deleted, they are still available on direct look-up.
        # However, they should not be listed under the dataset anymore.
        assert len(client.labels.list(data_set_external_ids=populated.dataset.external_id)) == 0
        relationships = client.relationships.list(source_external_ids=[populated.asset.external_id])
        assert len(relationships) == 0
        assert client.three_d.models.retrieve(id=populated.three_d.id) is None
        assert client.workflows.retrieve(external_id=populated.workflow.external_id, ignore_unknown_ids=True) is None
        assert client.transformations.retrieve(external_id=populated.transformation.external_id) is None
        assert client.extraction_pipelines.retrieve(external_id=populated.extraction_pipeline.external_id) is None
