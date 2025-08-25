from collections.abc import Iterator, Sequence
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.aggregations import CountValue
from cognite.client.data_classes.capabilities import (
    AllProjectsScope,
    DataModelInstancesAcl,
    DataModelsAcl,
    ProjectCapability,
    ProjectCapabilityList,
    TimeSeriesAcl,
)
from cognite.client.data_classes.data_modeling import (
    Boolean,
    ContainerId,
    MappedProperty,
    NodeId,
    NodeList,
    Text,
    View,
    ViewList,
)
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFile, CogniteTimeSeries
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.utils.useful_types import SequenceNotStr
from requests import Response

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.extended_filemetadata import (
    ExtendedFileMetadata,
    ExtendedFileMetadataList,
)
from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeries, ExtendedTimeSeriesList
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands import PurgeCommand


@pytest.fixture(scope="session")
def cognite_timeseries_2000_list() -> NodeList[CogniteTimeSeries]:
    return NodeList[CogniteTimeSeries](
        [
            CogniteTimeSeries(
                space="test_space",
                external_id=f"test_timeseries_{i}",
                version=1,
                last_updated_time=1,
                created_time=1,
                is_step=True,
                time_series_type="numeric",
            )
            for i in range(2000)
        ]
    )


@pytest.fixture(scope="session")
def cognite_files_2000_list() -> NodeList[CogniteFile]:
    return NodeList[CogniteFile](
        [
            CogniteFile(
                space="test_space",
                external_id=f"test_file_{i}",
                version=1,
                last_updated_time=1,
                created_time=1,
                name=f"test_file_{i}.txt",
                mime_type="text/plain",
            )
            for i in range(2000)
        ]
    )


@pytest.fixture()
def timeseries_by_node_id(
    cognite_timeseries_2000_list: NodeList[CogniteTimeSeries],
) -> dict[NodeId, ExtendedTimeSeries]:
    return {
        ts.as_id(): ExtendedTimeSeries(
            id=i,
            external_id=ts.external_id,
            instance_id=ts.as_id(),
            is_string=ts.time_series_type == "string",
            is_step=ts.is_step,
            pending_instance_id=ts.as_id(),
        )
        for i, ts in enumerate(cognite_timeseries_2000_list)
    }


@pytest.fixture()
def files_by_node_id(
    cognite_files_2000_list: NodeList[CogniteFile],
) -> dict[NodeId, ExtendedFileMetadata]:
    return {
        file.as_id(): ExtendedFileMetadata(
            id=i,
            external_id=file.external_id,
            instance_id=file.as_id(),
            pending_instance_id=file.as_id(),
        )
        for i, file in enumerate(cognite_files_2000_list)
    }


@pytest.fixture()
def purge_ts_client(
    cognite_timeseries_2000_list: NodeList[CogniteTimeSeries], timeseries_by_node_id: dict[NodeId, ExtendedTimeSeries]
) -> Iterator[ToolkitClient]:
    timeseries_by_id = {ts.id: ts for ts in timeseries_by_node_id.values()}

    def retrieve_timeseries_mock(
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtendedTimeSeriesList:
        if ids is not None or external_ids is not None:
            raise ValueError("Unexpected call to retrieve_timeseries with ids or external_ids")
        if instance_ids is None:
            raise ValueError("instance_ids must be provided")
        return ExtendedTimeSeriesList([timeseries_by_node_id[node_id] for node_id in instance_ids])

    def unlink_instance_ids_mock(id: int | Sequence[int] | None = None, **_) -> ExtendedTimeSeriesList:
        if id is None:
            raise ValueError("id must be provided")
        if isinstance(id, int):
            id = [id]
        return ExtendedTimeSeriesList([timeseries_by_id[i] for i in id])

    with monkeypatch_toolkit_client() as client:
        client.iam.token.inspect.return_value = TokenInspection(
            "123",
            projects=[],
            capabilities=ProjectCapabilityList(
                [
                    ProjectCapability(
                        capability=DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.All()),
                        project_scope=AllProjectsScope(),
                    ),
                    ProjectCapability(
                        capability=DataModelInstancesAcl(
                            [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write],
                            DataModelInstancesAcl.Scope.All(),
                        ),
                        project_scope=AllProjectsScope(),
                    ),
                    ProjectCapability(
                        capability=TimeSeriesAcl(
                            [TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write], TimeSeriesAcl.Scope.All()
                        ),
                        project_scope=AllProjectsScope(),
                    ),
                ]
            ),
        )
        client.data_modeling.views.retrieve.return_value = ViewList(
            [
                View(
                    "cdf_cdm",
                    "CogniteTimeSeries",
                    "v1",
                    {
                        "isStep": MappedProperty(
                            ContainerId("cdf_cdm", "CogniteTimeSeries"), "isStep", Boolean(), False, False, True
                        )
                    },
                    1,
                    1,
                    None,
                    None,
                    None,
                    None,
                    True,
                    "node",
                    is_global=True,
                )
            ]
        )
        response = MagicMock(spec=Response)
        response.json.return_value = {"items": [ts.dump() for ts in cognite_timeseries_2000_list]}
        client.post.return_value = response
        client.data_modeling.instances.aggregate.return_value = CountValue(
            "externalId", len(cognite_timeseries_2000_list)
        )
        client.time_series.retrieve_multiple.side_effect = retrieve_timeseries_mock
        client.time_series.unlink_instance_ids.side_effect = unlink_instance_ids_mock
        yield client


@pytest.fixture()
def purge_file_client(
    cognite_files_2000_list: NodeList[CogniteFile], files_by_node_id: dict[NodeId, ExtendedFileMetadata]
) -> Iterator[ToolkitClient]:
    files_by_id = {file.id: file for file in files_by_node_id.values()}

    def retrieve_files_mock(
        ids: Sequence[int] | None = None,
        external_ids: SequenceNotStr[str] | None = None,
        instance_ids: Sequence[NodeId] | None = None,
        ignore_unknown_ids: bool = False,
    ) -> ExtendedFileMetadataList:
        if ids is not None or external_ids is not None:
            raise ValueError("Unexpected call to retrieve_files with ids or external_ids")
        if instance_ids is None:
            raise ValueError("instance_ids must be provided")
        return ExtendedFileMetadataList([files_by_node_id[node_id] for node_id in instance_ids])

    def unlink_instance_ids_mock(id: int | Sequence[int] | None = None, **_) -> ExtendedFileMetadataList:
        if id is None:
            raise ValueError("id must be provided")
        if isinstance(id, int):
            id = [id]
        return ExtendedFileMetadataList([files_by_id[i] for i in id])

    with monkeypatch_toolkit_client() as client:
        client.iam.token.inspect.return_value = TokenInspection(
            "123",
            projects=[],
            capabilities=ProjectCapabilityList(
                [
                    ProjectCapability(
                        capability=DataModelsAcl([DataModelsAcl.Action.Read], DataModelsAcl.Scope.All()),
                        project_scope=AllProjectsScope(),
                    ),
                    ProjectCapability(
                        capability=DataModelInstancesAcl(
                            [DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write],
                            DataModelInstancesAcl.Scope.All(),
                        ),
                        project_scope=AllProjectsScope(),
                    ),
                ]
            ),
        )
        client.data_modeling.views.retrieve.return_value = ViewList(
            [
                View(
                    "cdf_cdm",
                    "CogniteFile",
                    "v1",
                    {
                        "mimeType": MappedProperty(
                            ContainerId("cdf_cdm", "CogniteFile"), "mimeType", Text(), False, False, True
                        )
                    },
                    1,
                    1,
                    None,
                    None,
                    None,
                    None,
                    True,
                    "node",
                    is_global=True,
                )
            ]
        )
        response = MagicMock(spec=Response)
        response.json.return_value = {"items": [file.dump() for file in cognite_files_2000_list]}
        client.post.return_value = response
        client.data_modeling.instances.aggregate.return_value = CountValue("externalId", len(cognite_files_2000_list))
        client.files.retrieve_multiple.side_effect = retrieve_files_mock
        client.files.unlink_instance_ids.side_effect = unlink_instance_ids_mock
        yield client


class TestPurgeInstances:
    def test_purge_timeseries_dry_run_unlink(
        self, purge_ts_client: ToolkitClient, timeseries_by_node_id: dict[NodeId, ExtendedTimeSeries]
    ) -> None:
        expected_node_ids = set(timeseries_by_node_id.keys())
        cmd = PurgeCommand(silent=True)
        cmd.instances(
            client=purge_ts_client,
            view=["cdf_cdm", "CogniteTimeSeries", "v1"],
            instance_space=None,
            dry_run=True,
            auto_yes=True,
            unlink=True,
        )

        assert purge_ts_client.time_series.retrieve_multiple.call_count == 2
        actual_node_ids = {
            node_id
            for call in purge_ts_client.time_series.retrieve_multiple.call_args_list
            for node_id in call[1]["instance_ids"]
        }
        assert actual_node_ids == expected_node_ids
        assert purge_ts_client.data_modeling.instances.delete_fast.call_count == 0

    def test_purge_timeseries_dry_run(self, purge_ts_client: ToolkitClient) -> None:
        cmd = PurgeCommand(silent=True)
        cmd.instances(
            client=purge_ts_client,
            view=["cdf_cdm", "CogniteTimeSeries", "v1"],
            instance_space=None,
            dry_run=True,
            auto_yes=True,
            unlink=False,
        )

        assert purge_ts_client.time_series.unlink_instance_ids.call_count == 0
        assert purge_ts_client.data_modeling.instances.delete_fast.call_count == 0

    def test_purge_timeseries_unlink(
        self, purge_ts_client: ToolkitClient, timeseries_by_node_id: dict[NodeId, ExtendedTimeSeries]
    ) -> None:
        expected_node_ids = set(timeseries_by_node_id.keys())
        expected_ids = {ts.id for ts in timeseries_by_node_id.values()}
        cmd = PurgeCommand(silent=True)
        cmd.instances(
            client=purge_ts_client,
            view=["cdf_cdm", "CogniteTimeSeries", "v1"],
            instance_space=None,
            dry_run=False,
            auto_yes=True,
            unlink=True,
        )

        assert purge_ts_client.time_series.unlink_instance_ids.call_count == 2
        actual_ids = {
            node_id
            for call in purge_ts_client.time_series.unlink_instance_ids.call_args_list
            for node_id in call[1]["id"]
        }
        assert actual_ids == expected_ids
        assert purge_ts_client.data_modeling.instances.delete_fast.call_count == 2
        actual_node_ids = {
            node_id
            for call in purge_ts_client.data_modeling.instances.delete_fast.call_args_list
            for node_id in call[0][0]
        }
        assert actual_node_ids == expected_node_ids

    def test_purge_timeseries(
        self, purge_ts_client: ToolkitClient, timeseries_by_node_id: dict[NodeId, ExtendedTimeSeries]
    ) -> None:
        expected_node_ids = set(timeseries_by_node_id.keys())

        cmd = PurgeCommand(silent=True)
        cmd.instances(
            client=purge_ts_client,
            view=["cdf_cdm", "CogniteTimeSeries", "v1"],
            instance_space=None,
            dry_run=False,
            auto_yes=True,
            unlink=False,
        )

        assert purge_ts_client.time_series.unlink_instance_ids.call_count == 0
        actual_node_ids = {
            node_id
            for call in purge_ts_client.data_modeling.instances.delete_fast.call_args_list
            for node_id in call[0][0]
        }
        assert purge_ts_client.data_modeling.instances.delete_fast.call_count == 2
        assert actual_node_ids == expected_node_ids

    def test_purge_files_dry_run_unlink(
        self, purge_file_client: ToolkitClient, files_by_node_id: dict[NodeId, ExtendedFileMetadata]
    ) -> None:
        expected_node_ids = set(files_by_node_id.keys())
        cmd = PurgeCommand(silent=True)
        cmd.instances(
            client=purge_file_client,
            view=["cdf_cdm", "CogniteFile", "v1"],
            instance_space=None,
            dry_run=True,
            auto_yes=True,
            unlink=True,
        )

        assert purge_file_client.files.retrieve_multiple.call_count == 2
        actual_node_ids = {
            node_id
            for call in purge_file_client.files.retrieve_multiple.call_args_list
            for node_id in call[1]["instance_ids"]
        }
        assert actual_node_ids == expected_node_ids
        assert purge_file_client.data_modeling.instances.delete_fast.call_count == 0

    def test_purge_files_dry_run(self, purge_file_client: ToolkitClient) -> None:
        cmd = PurgeCommand(silent=True)
        cmd.instances(
            client=purge_file_client,
            view=["cdf_cdm", "CogniteFile", "v1"],
            instance_space=None,
            dry_run=True,
            auto_yes=True,
            unlink=False,
        )

        assert purge_file_client.files.unlink_instance_ids.call_count == 0
        assert purge_file_client.data_modeling.instances.delete_fast.call_count == 0

    def test_purge_files_unlink(
        self, purge_file_client: ToolkitClient, files_by_node_id: dict[NodeId, ExtendedFileMetadata]
    ) -> None:
        expected_node_ids = set(files_by_node_id.keys())
        expected_ids = {file.id for file in files_by_node_id.values()}
        cmd = PurgeCommand(silent=True)
        cmd.instances(
            client=purge_file_client,
            view=["cdf_cdm", "CogniteFile", "v1"],
            instance_space=None,
            dry_run=False,
            auto_yes=True,
            unlink=True,
        )

        assert purge_file_client.files.unlink_instance_ids.call_count == 2
        actual_ids = {
            node_id for call in purge_file_client.files.unlink_instance_ids.call_args_list for node_id in call[1]["id"]
        }
        assert actual_ids == expected_ids
        assert purge_file_client.data_modeling.instances.delete_fast.call_count == 2
        actual_node_ids = {
            node_id
            for call in purge_file_client.data_modeling.instances.delete_fast.call_args_list
            for node_id in call[0][0]
        }
        assert actual_node_ids == expected_node_ids

    def test_purge_files(
        self, purge_file_client: ToolkitClient, files_by_node_id: dict[NodeId, ExtendedFileMetadata]
    ) -> None:
        expected_node_ids = set(files_by_node_id.keys())

        cmd = PurgeCommand(silent=True)
        cmd.instances(
            client=purge_file_client,
            view=["cdf_cdm", "CogniteFile", "v1"],
            instance_space=None,
            dry_run=False,
            auto_yes=True,
            unlink=False,
        )

        assert purge_file_client.files.unlink_instance_ids.call_count == 0
        actual_node_ids = {
            node_id
            for call in purge_file_client.data_modeling.instances.delete_fast.call_args_list
            for node_id in call[0][0]
        }
        assert purge_file_client.data_modeling.instances.delete_fast.call_count == 2
        assert actual_node_ids == expected_node_ids
