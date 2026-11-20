from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, WorkflowExecutionId
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import FunctionCallResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import FunctionScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, SessionsAcl
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import HostedExtractorJobResponse
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    TransformationJobResponse,
    TransformationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import (
    WorkflowExecutionDetailedResponse,
    WorkflowExecutionResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionResponse
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.resource_ios import TransformationIO, WorkflowIO
from cognite_toolkit._cdf_tk.rules import FailingJobRuleSet
from cognite_toolkit._cdf_tk.rules._failing_jobs import FailingJob
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms

NOW = datetime_to_ms(datetime.now(UTC))
RECENT = NOW - int(timedelta(days=1).total_seconds() * 1000)
OLD = NOW - int(timedelta(days=30).total_seconds() * 1000)


def _execution(
    id_: str, created_time: int, reason: str | None = None, status: str = "FAILED"
) -> WorkflowExecutionResponse:
    return WorkflowExecutionResponse(
        id=id_,
        workflow_external_id="my_workflow",
        version="v1",
        status=status,
        created_time=created_time,
        reason_for_incompletion=reason,
    )


def _workflow_version() -> WorkflowVersionResponse:
    return WorkflowVersionResponse.model_validate(
        {
            "workflowExternalId": "my_workflow",
            "version": "v1",
            "createdTime": 0,
            "lastUpdatedTime": 0,
            "workflowDefinition": {
                "tasks": [
                    {
                        "externalId": "call_fn",
                        "type": "function",
                        "parameters": {"function": {"externalId": "fn_in_workflow"}},
                    },
                    {
                        "externalId": "run_tr",
                        "type": "transformation",
                        "parameters": {"transformation": {"externalId": "tr_in_workflow"}},
                    },
                ]
            },
        }
    )


def _function(id_: int, external_id: str, last_called: int | None) -> FunctionResponse:
    return FunctionResponse(
        id=id_, external_id=external_id, name=external_id, file_id=1, created_time=0, last_called=last_called
    )


def _call(
    id_: int, status: str, start_time: int, error: str | None = None, schedule_id: int | None = None
) -> FunctionCallResponse:
    return FunctionCallResponse(
        id=id_, status=status, start_time=start_time, function_id=4, error=error, schedule_id=schedule_id
    )


def _job(id_: int, status: str, created_time: int, error: str | None = None) -> TransformationJobResponse:
    return TransformationJobResponse.model_validate(
        {
            "id": id_,
            "uuid": str(id_),
            "status": status,
            "transformationId": 2,
            "transformationExternalId": "tr_failed",
            "sourceProject": "my_project",
            "destinationProject": "my_project",
            "destination": {"type": "assets"},
            "conflictMode": "upsert",
            "query": "SELECT 1",
            "ignoreNullFields": True,
            "createdTime": created_time,
            "error": error,
        }
    )


def _transformation(id_: int, external_id: str, **extra: Any) -> TransformationResponse:
    return TransformationResponse.model_validate(
        {
            "id": id_,
            "externalId": external_id,
            "name": external_id,
            "ignoreNullFields": True,
            "createdTime": 0,
            "lastUpdatedTime": 0,
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


def _hosted_job(external_id: str, status: str, job_format: dict[str, Any] | None = None) -> HostedExtractorJobResponse:
    return HostedExtractorJobResponse.model_validate(
        {
            "externalId": external_id,
            "destinationId": "my_destination",
            "sourceId": "my_source",
            "format": job_format or {"type": "cognite"},
            "status": status,
            "createdTime": 0,
            "lastUpdatedTime": 0,
        }
    )


def _client() -> MagicMock:
    client = MagicMock()
    client.workflows.executions.list.return_value = [
        _execution("e1", RECENT - 20, "Old failure"),
        _execution("e2", RECENT - 10, status="COMPLETED"),
        _execution("e3", RECENT - 5, "Old failure"),
        _execution("e4", RECENT, "Function task failed"),
    ]
    client.workflows.versions.retrieve.return_value = [_workflow_version()]
    client.functions.list.return_value = [
        _function(1, "fn_in_workflow", last_called=RECENT),
        _function(2, "fn_not_called_recently", last_called=OLD),
        _function(3, "fn_never_called", last_called=None),
        _function(4, "fn_failing", last_called=RECENT),
    ]
    client.functions.calls.list.return_value = [
        _call(1, "Completed", RECENT - 20),
        _call(2, "Failed", RECENT - 10, error="Old error"),
        _call(3, "Timeout", RECENT, error="Function timed out", schedule_id=7),
        _call(4, "Running", RECENT + 10),
    ]
    client.functions.schedules.retrieve.return_value = [
        FunctionScheduleResponse(
            id=7, name="hourly", cron_expression="0 * * * *", created_time=0, when="hourly", session_id=300
        )
    ]
    client.transformations.list.return_value = [
        _transformation(1, "tr_in_workflow", lastFinishedJob=_job(10, "Failed", RECENT, "Column not found").dump()),
        _transformation(
            2,
            "tr_failed",
            lastFinishedJob=_job(11, "Failed", RECENT, "Column not found").dump(),
            destination={"type": "raw", "database": "my_db", "table": "my_table"},
            sourceSession={"sessionId": 100, "projectName": "my_project"},
            destinationSession={"sessionId": 101, "projectName": "my_project"},
        ),
        _transformation(3, "tr_failed_long_ago", lastFinishedJob=_job(12, "Failed", OLD).dump()),
        _transformation(4, "tr_blocked", blocked={"reason": "Too many failures", "createdTime": OLD}),
        _transformation(5, "tr_healthy", lastFinishedJob=_job(13, "Completed", RECENT).dump()),
    ]
    client.transformations.jobs.iterate.return_value = [
        [_job(11, "Failed", RECENT), _job(14, "Completed", RECENT - 10), _job(15, "Failed", RECENT - 20)]
    ]
    client.hosted_extractors.jobs.list.return_value = [
        _hosted_job("job_running", "running"),
        _hosted_job("job_write_error", "cdf_write_error"),
    ]
    client.hosted_extractors.destinations.retrieve.return_value = [
        HostedExtractorDestinationResponse(
            external_id="my_destination", session_id=200, created_time=0, last_updated_time=0
        )
    ]
    return client


class TestFailingJobRuleSet:
    def test_load_failures(self) -> None:
        client = _client()
        rule = FailingJobRuleSet(client=client)

        failures = rule._load_failures(client)

        assert [
            (failure.job_type, str(failure.identifier), failure.reason, failure.failed_executions)
            for failure in failures
        ] == [
            ("Workflow", "workflowExternalId='my_workflow', version='v1'", "Function task failed", 3),
            ("Function", "externalId='fn_failing'", "Function timed out", 2),
            ("Transformation", "externalId='tr_failed'", "Column not found", 2),
            ("Transformation", "externalId='tr_blocked'", "Blocked: Too many failures", None),
            ("Hosted Extractor", "externalId='job_write_error'", "cdf_write_error", None),
        ]

    def test_only_lists_executions_of_recently_failing_resources(self) -> None:
        client = _client()
        rule = FailingJobRuleSet(client=client)

        rule._load_failures(client)

        assert {
            "function_calls": [call.kwargs["function_id"] for call in client.functions.calls.list.call_args_list],
            "transformation_jobs": [
                call.kwargs["transformation_id"] for call in client.transformations.jobs.iterate.call_args_list
            ],
        } == {"function_calls": [4], "transformation_jobs": [2]}

    def test_no_failed_workflow_executions_skips_retrieving_versions(self) -> None:
        client = _client()
        client.workflows.executions.list.return_value = []
        rule = FailingJobRuleSet(client=client)

        failures = rule._load_failing_workflows(client)

        assert (failures, client.workflows.versions.retrieve.called) == ([], False)

    def test_workflow_succeeding_after_failure_is_not_reported(self) -> None:
        client = _client()
        client.workflows.executions.list.return_value = [
            _execution("e1", RECENT - 10, "Old failure"),
            _execution("e2", RECENT, status="COMPLETED"),
        ]
        rule = FailingJobRuleSet(client=client)

        failures = rule._load_failing_workflows(client)

        assert (failures, client.workflows.versions.retrieve.called) == ([], False)

    def test_function_succeeding_after_failure_is_not_reported(self) -> None:
        client = _client()
        client.functions.list.return_value = [_function(4, "fn_recovered", last_called=RECENT)]
        client.functions.calls.list.return_value = [
            _call(1, "Failed", RECENT - 10, error="Old error"),
            _call(2, "Completed", RECENT),
        ]
        rule = FailingJobRuleSet(client=client)

        assert rule._load_failing_functions(client, skip=set()) == []

    def test_count_failed_jobs_stops_paging_at_lookback_period(self) -> None:
        client = MagicMock()
        pages_read: list[int] = []

        def pages(**_: Any) -> Iterator[list[TransformationJobResponse]]:
            for page_no, page in enumerate(
                [
                    [_job(1, "Failed", RECENT), _job(2, "Completed", RECENT)],
                    [_job(3, "Failed", RECENT), _job(4, "Failed", OLD)],
                    [_job(5, "Failed", OLD)],
                ]
            ):
                pages_read.append(page_no)
                yield page

        client.transformations.jobs.iterate.side_effect = pages
        rule = FailingJobRuleSet(client=client)

        count = rule._count_failed_jobs(client, transformation_id=2)

        assert (count, pages_read) == (2, [0, 1])

    def test_workflow_failure_keeps_dependencies(self) -> None:
        client = _client()
        rule = FailingJobRuleSet(client=client)

        (failure,) = rule._load_failing_workflows(client)

        assert {(io.__name__, identifier) for io, identifier in failure.dependencies} == {
            ("WorkflowIO", ExternalId(external_id="my_workflow")),
            ("FunctionIO", ExternalId(external_id="fn_in_workflow")),
            ("TransformationIO", ExternalId(external_id="tr_in_workflow")),
        }

    def test_load_failures_keeps_dependencies_and_sessions(self) -> None:
        client = _client()
        rule = FailingJobRuleSet(client=client)

        failures = rule._load_failures(client)

        assert {
            str(failure.identifier): (
                {(io.__name__, str(identifier)) for io, identifier in failure.dependencies},
                failure.session_ids,
            )
            for failure in failures
            if failure.job_type != "Workflow"
        } == {
            "externalId='fn_failing'": (set(), {300}),
            "externalId='tr_failed'": (
                {("RawDatabaseIO", "name='my_db'"), ("RawTableIO", "my_db.my_table")},
                {100, 101},
            ),
            "externalId='tr_blocked'": (set(), set()),
            "externalId='job_write_error'": (
                {
                    ("HostedExtractorSourceIO", "externalId='my_source'"),
                    ("HostedExtractorDestinationIO", "externalId='my_destination'"),
                },
                {200},
            ),
        }

    def test_diagnose_failures_sets_suggested_fix_for_missing_dependencies_and_invalid_sessions(self) -> None:
        client = MagicMock()
        client.workflows.retrieve.return_value = [
            WorkflowResponse(external_id="my_workflow", created_time=0, last_updated_time=0)
        ]
        client.transformations.retrieve.return_value = []
        client.token.verify_acls.return_value = []
        client.sessions.retrieve.return_value = [
            SessionResponse(id=1, status="ACTIVE"),
            SessionResponse(id=2, status="EXPIRED"),
        ]
        broken = FailingJob(
            job_type="Workflow",
            job=_workflow_version(),
            url="/workflows/my_workflow/executions/",
            dependencies={
                (WorkflowIO, ExternalId(external_id="my_workflow")),
                (TransformationIO, ExternalId(external_id="tr_missing")),
            },
            session_ids={2},
        )
        healthy = FailingJob(
            job_type="Transformation",
            job=_transformation(1, "tr_healthy"),
            url="/transformations/1/",
            reason="Column not found",
            dependencies={(WorkflowIO, ExternalId(external_id="my_workflow"))},
            session_ids={1},
        )
        rule = FailingJobRuleSet(client=client)

        exceptions = list(rule._diagnose_failures(client, [broken, healthy]))

        assert {
            "exceptions": exceptions,
            "broken": (broken.reason, broken.suggested_fix),
            "healthy": (healthy.reason, healthy.suggested_fix),
        } == {
            "exceptions": [],
            "broken": (
                "It uses resources that do not exist in CDF. Its credentials are no longer valid.",
                "Create Transformation 'tr_missing' in CDF, or remove the references to them. "
                "Update the credentials used by the workflow, as session 2 is expired.",
            ),
            "healthy": ("Column not found", None),
        }

    def test_diagnose_failures_without_sessions_acl_does_not_report_invalid_credentials(self) -> None:
        client = MagicMock()
        client.token.verify_acls.return_value = [SessionsAcl(actions=["LIST"], scope=AllScope())]
        failure = FailingJob(
            job_type="Transformation",
            job=_transformation(1, "tr_failed"),
            url="/transformations/1/",
            session_ids={1},
        )
        rule = FailingJobRuleSet(client=client)

        exceptions = list(rule._diagnose_failures(client, [failure]))

        assert {
            "exceptions": exceptions,
            "failure": (failure.reason, failure.suggested_fix),
            "sessions_retrieved": client.sessions.retrieve.called,
        } == {
            "exceptions": [],
            "failure": (
                None,
                "Couldn't verify the credentials used by the transformation. "
                "Add sessionsAcl:LIST to your credentials to check them.",
            ),
            "sessions_retrieved": False,
        }

    @pytest.mark.parametrize(
        "destination, expected",
        [
            pytest.param({"type": "asset_hierarchy"}, {"asset hierarchy (asset-centric)"}, id="asset-centric"),
            pytest.param(
                {
                    "type": "instances",
                    "dataModel": {
                        "space": "my_space",
                        "externalId": "MyModel",
                        "version": "v1",
                        "destinationType": "MyView",
                    },
                    "instanceSpace": "my_instances",
                },
                {
                    "data model my_space:MyModel(version=v1)",
                    "instances of type MyView in space my_space",
                    "instance space my_instances",
                },
                id="data-model",
            ),
            pytest.param(
                {
                    "type": "edges",
                    "view": {"space": "my_space", "externalId": "MyView", "version": "v1"},
                    "edgeType": {"space": "my_space", "externalId": "relates"},
                },
                {"edges of view my_space:MyView(version=v1)", "edges of type my_space:relates"},
                id="view",
            ),
            pytest.param({"type": "raw", "database": "db", "table": "tbl"}, {"RAW table db.tbl"}, id="raw"),
            pytest.param({"type": "sequence_rows", "externalId": "seq"}, {"rows of sequence seq"}, id="sequence"),
            pytest.param({"type": "new_type"}, {"destination of type new_type"}, id="unknown"),
        ],
    )
    def test_get_transformation_impact(self, destination: dict[str, Any], expected: set[str]) -> None:
        transformation = _transformation(1, "tr", destination=destination)

        assert FailingJobRuleSet._get_transformation_impact(transformation.destination) == expected

    @pytest.mark.parametrize(
        "job_format, expected",
        [
            pytest.param(
                {"type": "cognite", "dataModels": {"space": "my_space"}},
                {"resources in data set with id 42", "instances in space my_space"},
                id="data-models",
            ),
            pytest.param(
                {"type": "custom", "mappingId": "my_mapping"},
                {"resources in data set with id 42", "resources written by mapping my_mapping"},
                id="custom",
            ),
        ],
    )
    def test_get_hosted_extractor_impact(self, job_format: dict[str, Any], expected: set[str]) -> None:
        job = _hosted_job("job", "cdf_write_error", job_format)
        destination = HostedExtractorDestinationResponse(
            external_id="my_destination", target_data_set_id=42, created_time=0, last_updated_time=0
        )

        assert FailingJobRuleSet._get_hosted_extractor_impact(job, destination) == expected

    def test_set_workflow_impact_from_incomplete_tasks(self) -> None:
        client = MagicMock()
        client.workflows.executions.retrieve.return_value = [
            WorkflowExecutionDetailedResponse.model_validate(
                {
                    "id": "e1",
                    "workflowExternalId": "my_workflow",
                    "version": "v1",
                    "status": "FAILED",
                    "createdTime": RECENT,
                    "workflowDefinition": {
                        "tasks": [
                            {
                                "externalId": "done_tr",
                                "type": "transformation",
                                "parameters": {"transformation": {"externalId": "tr_done"}},
                            },
                            {
                                "externalId": "failed_tr",
                                "type": "transformation",
                                "parameters": {"transformation": {"externalId": "tr_failed"}},
                            },
                            {
                                "externalId": "call_fn",
                                "type": "function",
                                "parameters": {"function": {"externalId": "fn"}},
                            },
                            {
                                "externalId": "sub",
                                "type": "subworkflow",
                                "parameters": {
                                    "subworkflow": {
                                        "tasks": [
                                            {
                                                "externalId": "write_dps",
                                                "type": "cdf",
                                                "parameters": {
                                                    "cdfRequest": {
                                                        "resourcePath": "/timeseries/data",
                                                        "method": "POST",
                                                    }
                                                },
                                            },
                                            {
                                                "externalId": "list_ts",
                                                "type": "cdf",
                                                "parameters": {
                                                    "cdfRequest": {"resourcePath": "/timeseries/list", "method": "POST"}
                                                },
                                            },
                                        ]
                                    }
                                },
                            },
                        ]
                    },
                    "executedTasks": [
                        {"id": "t1", "externalId": "done_tr", "status": "COMPLETED"},
                        {"id": "t2", "externalId": "failed_tr", "status": "FAILED"},
                        {"id": "t3", "externalId": "call_fn", "status": "FAILED"},
                    ],
                }
            )
        ]
        client.transformations.retrieve.return_value = [
            _transformation(1, "tr_failed", destination={"type": "raw", "database": "db", "table": "tbl"})
        ]
        failure = FailingJob(
            job_type="Workflow",
            job=_workflow_version(),
            url="/workflows/my_workflow/executions/",
            execution_id=WorkflowExecutionId(id="e1"),
        )
        rule = FailingJobRuleSet(client=client)

        exceptions = list(rule._set_workflow_impact(client, [failure]))

        assert (exceptions, failure.impact) == (
            [],
            {"RAW table db.tbl", "target of CDF request POST /timeseries/data"},
        )

    def test_diagnose_failures_reports_failing_token_inspection(self) -> None:
        client = MagicMock()
        client.token.verify_acls.side_effect = AuthorizationError("Token inspection failed")
        failure = FailingJob(
            job_type="Transformation",
            job=_transformation(1, "tr_failed"),
            url="/transformations/1/",
            session_ids={1},
        )
        rule = FailingJobRuleSet(client=client)

        exceptions = list(rule._diagnose_failures(client, [failure]))

        assert [exception.message for exception in exceptions] == [
            "Failed to verify sessions in CDF: Token inspection failed"
        ]
