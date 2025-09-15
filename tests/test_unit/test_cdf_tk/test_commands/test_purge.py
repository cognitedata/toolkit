from collections.abc import Iterator

import pytest
import responses
from cognite.client.data_classes.capabilities import (
    DataModelInstancesAcl,
    DataModelsAcl,
    FilesAcl,
    TimeSeriesAcl,
)
from cognite.client.data_classes.data_modeling import (
    NodeId,
    NodeList,
    ViewId,
)
from cognite.client.data_classes.data_modeling.cdm.v1 import CogniteFile, CogniteTimeSeries

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.data_classes.extended_filemetadata import (
    ExtendedFileMetadata,
)
from cognite_toolkit._cdf_tk.client.data_classes.extended_timeseries import ExtendedTimeSeries
from cognite_toolkit._cdf_tk.commands import PurgeCommand
from cognite_toolkit._cdf_tk.storageio import InstanceViewSelector


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
def purge_responses(
    rsps: responses.RequestsMock, toolkit_config: ToolkitClientConfig
) -> Iterator[responses.RequestsMock]:
    config = toolkit_config
    rsps.add(
        responses.GET,
        f"{config.base_url}/api/v1/token/inspect",
        json={
            "subject": "123",
            "projects": [],
            "capabilities": [
                {
                    "projectScope": {"allProjects": {}},
                    **TimeSeriesAcl(
                        actions=[TimeSeriesAcl.Action.Read, TimeSeriesAcl.Action.Write],
                        scope=TimeSeriesAcl.Scope.All(),
                    ).dump(),
                },
                {
                    "projectScope": {"allProjects": {}},
                    **DataModelsAcl(actions=[DataModelsAcl.Action.Read], scope=DataModelsAcl.Scope.All()).dump(),
                },
                {
                    "projectScope": {"allProjects": {}},
                    **DataModelInstancesAcl(
                        actions=[DataModelInstancesAcl.Action.Read, DataModelInstancesAcl.Action.Write],
                        scope=DataModelInstancesAcl.Scope.All(),
                    ).dump(),
                },
                {
                    "projectScope": {"allProjects": {}},
                    **FilesAcl(
                        actions=[FilesAcl.Action.Read, FilesAcl.Action.Write], scope=FilesAcl.Scope.All()
                    ).dump(),
                },
            ],
        },
    )
    yield rsps


@pytest.fixture()
def purge_client(toolkit_config: ToolkitClientConfig) -> Iterator[ToolkitClient]:
    config = toolkit_config
    client = ToolkitClient(config, enable_set_pending_ids=True)
    yield client


class TestPurgeInstances:
    @pytest.mark.parametrize(
        "dry_run,unlink,instance_type",
        [
            pytest.param(True, True, "timeseries", id="dry run with unlink timeseries"),
            pytest.param(True, True, "files", id="dry run with unlink files"),
            pytest.param(True, False, "timeseries", id="dry run without unlink timeseries"),
            pytest.param(True, False, "files", id="dry run without unlink files"),
            pytest.param(False, True, "timeseries", id="purge with unlink timeseries"),
            pytest.param(False, True, "files", id="purge with unlink files"),
            pytest.param(False, False, "timeseries", id="purge without unlink timeseries"),
            pytest.param(False, False, "files", id="purge without unlink files"),
        ],
    )
    def test_purge(
        self,
        dry_run: bool,
        unlink: bool,
        instance_type: str,
        purge_client: ToolkitClient,
        purge_responses: responses.RequestsMock,
        cognite_timeseries_2000_list: NodeList[CogniteTimeSeries],
        timeseries_by_node_id: dict[NodeId, ExtendedTimeSeries],
        cognite_files_2000_list: NodeList[CogniteFile],
        files_by_node_id: dict[NodeId, ExtendedFileMetadata],
    ) -> None:
        config = purge_client.config
        rsps = purge_responses
        instances = cognite_timeseries_2000_list if instance_type == "timeseries" else cognite_files_2000_list
        client = purge_client
        rsps.add(
            responses.POST,
            config.create_api_url("/models/instances/aggregate"),
            json={
                "items": [
                    {
                        "instanceType": "node",
                        "aggregates": [{"aggregate": "count", "value": len(instances), "property": "externalId"}],
                    }
                ]
            },
        )
        rsps.add(
            responses.POST,
            config.create_api_url("/models/instances/list"),
            json={"items": [instance.dump() for instance in instances]},
        )
        ts_objects = [ts.dump() for ts in timeseries_by_node_id.values()] if instance_type == "timeseries" else []
        file_objects = [file.dump() for file in files_by_node_id.values()] if instance_type == "files" else []
        if unlink:
            rsps.add(responses.POST, config.create_api_url("/timeseries/byids"), json={"items": ts_objects})
            rsps.add(responses.POST, config.create_api_url("/files/byids"), json={"items": file_objects})
        if unlink and not dry_run and instance_type == "timeseries":
            rsps.add(
                responses.POST,
                config.create_api_url("/timeseries/unlink-instance-ids"),
                json={"items": [ts.dump() for ts in timeseries_by_node_id.values()]},
            )
        if unlink and not dry_run and instance_type == "files":
            rsps.add(
                responses.POST,
                config.create_api_url("/files/unlink-instance-ids"),
                json={"items": [file.dump() for file in files_by_node_id.values()]},
            )
        if not dry_run:
            rsps.add(
                responses.POST,
                config.create_api_url("/models/instances/delete"),
                json={"items": [instance.as_id().dump() for instance in instances]},
            )

        cmd = PurgeCommand(silent=True)
        cmd.instances(
            client,
            InstanceViewSelector(view=ViewId(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1")),
            dry_run=dry_run,
            auto_yes=True,
            unlink=unlink,
            verbose=False,
        )
