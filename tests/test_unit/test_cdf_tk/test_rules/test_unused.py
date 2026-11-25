from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, RawDatabaseId, RawTableId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceAggregateResponse,
    SpaceResponse,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import HostedExtractorJobResponse
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWDatabaseResponse, RAWTableResponse
from cognite_toolkit._cdf_tk.client.resource_classes.statistics import SpaceStatisticsResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowResponse
from cognite_toolkit._cdf_tk.resource_ios import (
    FunctionIO,
    HostedExtractorJobIO,
    HostedExtractorMappingIO,
    RawDatabaseIO,
    RawTableIO,
    SpaceIO,
    TransformationIO,
    ViewIO,
    WorkflowIO,
)
from cognite_toolkit._cdf_tk.rules import UnusedRuleSet
from cognite_toolkit._cdf_tk.rules._unused import _UnusedResourceTypes
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms

NOW = datetime_to_ms(datetime.now(UTC))
OLD = NOW - int(timedelta(days=30).total_seconds() * 1000)
CREATED_BEFORE = NOW - int(timedelta(days=7).total_seconds() * 1000)


def _transformation(external_id: str, **extra: Any) -> TransformationResponse:
    return TransformationResponse.model_validate(
        {
            "id": 1,
            "externalId": external_id,
            "name": external_id,
            "ignoreNullFields": True,
            "createdTime": OLD,
            "lastUpdatedTime": OLD,
            "query": "SELECT 1",
            "isPublic": True,
            "conflictMode": "upsert",
            "destination": {"type": "assets"},
            "owner": "me",
            "ownerIsCurrentUser": True,
            "hasSourceOidcCredentials": False,
            "hasDestinationOidcCredentials": False,
            **extra,
        }
    )


def _hosted_job(external_id: str, created_time: int) -> HostedExtractorJobResponse:
    return HostedExtractorJobResponse.model_validate(
        {
            "externalId": external_id,
            "destinationId": "my_destination",
            "sourceId": "my_source",
            "format": {"type": "cognite"},
            "createdTime": created_time,
            "lastUpdatedTime": created_time,
        }
    )


def _view(external_id: str, used_for: str, queryable: bool = True, has_containers: bool = True) -> ViewResponse:
    return ViewResponse.model_validate(
        {
            "space": "my_space",
            "externalId": external_id,
            "version": "v1",
            "createdTime": OLD,
            "lastUpdatedTime": OLD,
            "writable": True,
            "queryable": queryable,
            "usedFor": used_for,
            "isGlobal": False,
            "properties": {},
            "mappedContainers": (
                [{"space": "my_space", "externalId": "my_container", "type": "container"}] if has_containers else []
            ),
            "implements": [],
        }
    )


def _space_statistics(space: str, nodes: int = 0) -> SpaceStatisticsResponse:
    return SpaceStatisticsResponse(
        space=space,
        containers=0,
        views=0,
        data_models=0,
        edges=0,
        soft_deleted_edges=0,
        nodes=nodes,
        soft_deleted_nodes=0,
    )


def _count(value: int) -> InstanceAggregateResponse:
    return InstanceAggregateResponse.model_validate(
        {
            "items": [
                {
                    "instanceType": "node",
                    "aggregates": [{"aggregate": "count", "property": "externalId", "value": value}],
                }
            ]
        }
    )


def _unused_by_kind(client: MagicMock) -> dict[str, list[Any]]:
    return {
        unused.type.kind: list(unused.identifiers)
        for unused in UnusedRuleSet(client)._load_unused(client, CREATED_BEFORE)
        if isinstance(unused, _UnusedResourceTypes)
    }


class TestUnusedRuleSet:
    def test_jobs_created_before_and_never_run_are_unused(self) -> None:
        client = MagicMock()
        client.workflows.list.return_value = [
            WorkflowResponse(external_id="wf_never_run", created_time=OLD, last_updated_time=OLD),
            WorkflowResponse(external_id="wf_run", created_time=OLD, last_updated_time=OLD),
            WorkflowResponse(external_id="wf_new", created_time=NOW, last_updated_time=NOW),
        ]
        client.workflows.executions.list.side_effect = lambda filter, limit: (
            [] if filter.workflow_filters[0].external_id == "wf_never_run" else [MagicMock()]
        )
        client.transformations.list.return_value = [
            _transformation("tr_never_run"),
            _transformation(
                "tr_run",
                lastFinishedJob={
                    "id": 1,
                    "uuid": "1",
                    "status": "Completed",
                    "transformationId": 1,
                    "transformationExternalId": "tr_run",
                    "sourceProject": "my_project",
                    "destinationProject": "my_project",
                    "destination": {"type": "assets"},
                    "conflictMode": "upsert",
                    "query": "SELECT 1",
                    "ignoreNullFields": True,
                    "createdTime": NOW,
                },
            ),
        ]
        client.functions.list.return_value = [
            FunctionResponse(id=1, external_id="fn_never_called", name="a", file_id=1, created_time=OLD),
            FunctionResponse(id=2, external_id="fn_called", name="b", file_id=1, created_time=OLD, last_called=NOW),
            FunctionResponse(id=3, name="fn_new", file_id=1, created_time=NOW),
        ]
        client.hosted_extractors.jobs.list.return_value = [
            _hosted_job("job_never_run", OLD),
            _hosted_job("job_new", NOW),
        ]
        client.hosted_extractors.jobs.list_logs.return_value = []

        unused = _unused_by_kind(client)

        assert unused == {
            WorkflowIO.kind: [ExternalId(external_id="wf_never_run")],
            TransformationIO.kind: [ExternalId(external_id="tr_never_run")],
            FunctionIO.kind: [ExternalId(external_id="fn_never_called")],
            HostedExtractorJobIO.kind: [ExternalId(external_id="job_never_run")],
        }

    def test_views_without_instances_are_unused(self) -> None:
        client = MagicMock()
        client.views.list.return_value = [
            _view("empty", "node"),
            _view("populated", "all"),
            _view("records", "record"),
            _view("not_queryable", "node", queryable=False),
            _view("no_containers", "node", has_containers=False),
        ]
        client.instances.aggregate.side_effect = lambda view, instance_type, aggregates: _count(
            1 if view.external_id == "populated" and instance_type == "edge" else 0
        )

        assert _unused_by_kind(client) == {ViewIO.kind: [ViewId(space="my_space", external_id="empty", version="v1")]}

    def test_empty_non_global_spaces_are_unused(self) -> None:
        client = MagicMock()
        client.statistics.spaces.list.return_value = [
            _space_statistics("empty"),
            _space_statistics("populated", nodes=1),
            _space_statistics("cdf_cdm"),
        ]
        client.spaces.retrieve.return_value = [
            SpaceResponse(space="empty", created_time=OLD, last_updated_time=OLD, is_global=False),
            SpaceResponse(space="cdf_cdm", created_time=OLD, last_updated_time=OLD, is_global=True),
        ]

        assert _unused_by_kind(client) == {SpaceIO.kind: [SpaceId(space="empty")]}

    def test_raw_databases_without_tables_and_tables_without_rows_are_unused(self) -> None:
        client = MagicMock()
        client.raw.databases.list.return_value = [
            RAWDatabaseResponse(name="empty_db", created_time=OLD),
            RAWDatabaseResponse(name="my_db", created_time=OLD),
        ]
        client.raw.tables.list.side_effect = lambda db_name, limit: (
            []
            if db_name == "empty_db"
            else [
                RAWTableResponse(db_name=db_name, name="empty_table", created_time=OLD),
                RAWTableResponse(db_name=db_name, name="populated_table", created_time=OLD),
            ]
        )
        client.raw.tables.rows.list.side_effect = lambda db_name, table_name, limit, columns: (
            [MagicMock()] if table_name == "populated_table" else []
        )

        assert _unused_by_kind(client) == {
            RawDatabaseIO.kind: [RawDatabaseId(name="empty_db")],
            RawTableIO.kind: [RawTableId(db_name="my_db", name="empty_table")],
        }

    def test_failing_check_is_reported_without_stopping_other_checks(self) -> None:
        client = MagicMock()
        client.workflows.list.side_effect = ToolkitAPIError("Forbidden", code=403)
        client.hosted_extractors.jobs.list.return_value = [_hosted_job("job_never_run", OLD)]
        client.hosted_extractors.jobs.list_logs.return_value = []
        client.hosted_extractors.mappings.list.side_effect = ToolkitAPIError("Forbidden", code=403)

        results = list(UnusedRuleSet(client)._load_unused(client, CREATED_BEFORE))

        assert [(type(result).__name__, getattr(result, "source", None)) for result in results] == [
            ("InternalValidatorException", WorkflowIO.kind),
            ("_UnusedResourceTypes", None),
            ("InternalValidatorException", HostedExtractorMappingIO.kind),
        ]
