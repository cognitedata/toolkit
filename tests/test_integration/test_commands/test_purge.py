from collections.abc import Iterable
from datetime import datetime
from pathlib import Path

import pytest
from cognite.client.data_classes import FileMetadataWrite, TimeSeriesWrite
from cognite.client.data_classes.data_modeling import NodeId, Space
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFileApply, CogniteTimeSeriesApply
from cognite.client.utils import datetime_to_ms

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands import PurgeCommand
from cognite_toolkit._cdf_tk.storageio import InstanceFileSelector
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
