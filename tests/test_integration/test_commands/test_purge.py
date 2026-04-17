import contextlib
from collections.abc import Iterable
from dataclasses import dataclass

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
from cognite.client.exceptions import CogniteAPIError, CogniteNotFoundError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import PurgeCommand
from tests.test_integration.constants import RUN_UNIQUE_ID


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
def populated_datasets_3(toolkit_client: ToolkitClient) -> Iterable[PopulatedDataSet]:
    populated3 = create_populated_dataset(
        toolkit_client, name="toolkit_test_purge_dataset_3", external_id="toolkit_test_purge_dataset_3", no=3
    )
    yield populated3
    cleanup_populated_dataset(toolkit_client, populated3)


def create_populated_dataset(toolkit_client: ToolkitClient, name: str, external_id: str, no: int) -> PopulatedDataSet:
    client = toolkit_client
    dataset = DataSetWrite(name=name, external_id=external_id)
    created = client.data_sets.retrieve(external_id=dataset.external_id)
    if not created:
        # DataSet cannot be deleted, so we create it only once and reuse it
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

    relationship = RelationshipWrite(
        external_id=f"test_relationship_{RUN_UNIQUE_ID}_{no}",
        source_external_id=created_asset.external_id,
        target_external_id=created_event.external_id,
        source_type="asset",
        target_type="event",
        data_set_id=created.id,
    )
    created_relationship = client.relationships.create(relationship)

    three_d = ThreeDModelWrite(
        name=f"Test 3D Model {RUN_UNIQUE_ID}_{no}",
        data_set_id=created.id,
    )
    created_three_d = client.three_d.models.create(three_d)

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
    # Cleanup
    client.assets.delete(id=populated.asset.id, ignore_unknown_ids=True)
    client.events.delete(id=populated.event.id, ignore_unknown_ids=True)
    client.sequences.delete(id=populated.sequence.id, ignore_unknown_ids=True)
    client.time_series.delete(id=populated.timeseries.id, ignore_unknown_ids=True)
    client.files.delete(id=populated.file.id, ignore_unknown_ids=True)
    client.labels.delete(external_id=populated.label.external_id)
    client.relationships.delete(external_id=populated.relationships.external_id, ignore_unknown_ids=True)
    with contextlib.suppress(CogniteAPIError):
        client.three_d.models.delete(id=populated.three_d.id)
    client.workflows.delete(external_id=populated.workflow.external_id, ignore_unknown_ids=True)
    client.transformations.delete(id=populated.transformation.id, ignore_unknown_ids=True)
    with contextlib.suppress(CogniteNotFoundError):
        client.extraction_pipelines.delete(id=populated.extraction_pipeline.id)


class TestPurge:
    def test_purge_dataset_dry_run(self, toolkit_client: ToolkitClient, populated_datasets_3: PopulatedDataSet) -> None:
        client = toolkit_client
        populated = populated_datasets_3
        purge = PurgeCommand(silent=True)

        results = purge.dataset(
            client,
            selected_data_set_external_id=populated.dataset.external_id,
            archive_dataset=False,
            include_data=True,
            include_configurations=True,
            dry_run=True,
            auto_yes=True,
            verbose=False,
        )
        assert results.dry_run == 1

        # Data not deleted
        assert client.assets.retrieve(external_id=populated.asset.external_id) is not None
        assert client.events.retrieve(external_id=populated.event.external_id) is not None
        assert client.sequences.retrieve(external_id=populated.sequence.external_id) is not None
        assert client.time_series.retrieve(external_id=populated.timeseries.external_id) is not None
        assert client.files.retrieve(external_id=populated.file.external_id) is not None
        # Labels are not deleted, they are still available on direct look-up.
        # However, they should not be listed under the dataset anymore.
        assert len(client.labels.list(data_set_external_ids=populated.dataset.external_id)) >= 1
        relationships = client.relationships.list(source_external_ids=[populated.asset.external_id])
        assert len(relationships) == 1
        assert client.three_d.models.retrieve(id=populated.three_d.id) is not None
        # Configurations not deleted
        assert (
            client.workflows.retrieve(external_id=populated.workflow.external_id, ignore_unknown_ids=True) is not None
        )
        assert client.transformations.retrieve(external_id=populated.transformation.external_id) is not None
        assert client.extraction_pipelines.retrieve(external_id=populated.extraction_pipeline.external_id) is not None
