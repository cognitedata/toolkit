from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import FunctionCallResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import HostedExtractorJobResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    TransformationJobResponse,
    TransformationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import WorkflowExecutionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionResponse
from cognite_toolkit._cdf_tk.data_classes._insights import BuildError
from cognite_toolkit._cdf_tk.rules import FailingJobRuleSet
from cognite_toolkit._cdf_tk.rules._failing_jobs import FailingJob
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms

NOW = datetime_to_ms(datetime.now(UTC))
RECENT = NOW - int(timedelta(days=1).total_seconds() * 1000)
OLD = NOW - int(timedelta(days=30).total_seconds() * 1000)


def _execution(id_: str, created_time: int, reason: str) -> WorkflowExecutionResponse:
    return WorkflowExecutionResponse(
        id=id_,
        workflow_external_id="my_workflow",
        version="v1",
        status="FAILED",
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


def _call(id_: int, status: str, start_time: int, error: str | None = None) -> FunctionCallResponse:
    return FunctionCallResponse(id=id_, status=status, start_time=start_time, function_id=4, error=error)


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


def _hosted_job(external_id: str, status: str) -> HostedExtractorJobResponse:
    return HostedExtractorJobResponse.model_validate(
        {
            "externalId": external_id,
            "destinationId": "my_destination",
            "sourceId": "my_source",
            "format": {"type": "cognite"},
            "status": status,
            "createdTime": 0,
            "lastUpdatedTime": 0,
        }
    )


def _client() -> MagicMock:
    client = MagicMock()
    client.workflows.executions.list.return_value = [
        _execution("e1", RECENT - 10, "Old failure"),
        _execution("e2", RECENT, "Function task failed"),
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
        _call(3, "Timeout", RECENT, error="Function timed out"),
    ]
    client.transformations.list.return_value = [
        _transformation(1, "tr_in_workflow", lastFinishedJob=_job(10, "Failed", RECENT, "Column not found").dump()),
        _transformation(2, "tr_failed", lastFinishedJob=_job(11, "Failed", RECENT, "Column not found").dump()),
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
            ("Workflow", "workflowExternalId='my_workflow', version='v1'", "Function task failed", 2),
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

    def test_report_failures_uses_suggested_fix(self) -> None:
        rule = FailingJobRuleSet(client=_client())
        failure = FailingJob(
            job_type="Function",
            job=_function(4, "fn_failing", last_called=RECENT),
            reason="Unauthorized",
            url="/functions/4/",
            failed_executions=3,
            suggested_fix="Give the function credentials access to the dataset.",
        )

        insights = list(rule._report_failures([failure]))

        assert [
            (type(insight), insight.code, insight.fix, insight.source_url)
            for insight in insights
            if isinstance(insight, BuildError)
        ] == [
            (
                BuildError,
                "FAILING-JOB",
                "Give the function credentials access to the dataset.",
                "/functions/4/",
            )
        ]

    def test_workflow_failure_keeps_dependencies(self) -> None:
        client = _client()
        rule = FailingJobRuleSet(client=client)

        (failure,) = rule._load_failing_workflows(client)

        assert {(io.__name__, identifier) for io, identifier in failure.dependencies} == {
            ("WorkflowIO", ExternalId(external_id="my_workflow")),
            ("FunctionIO", ExternalId(external_id="fn_in_workflow")),
            ("TransformationIO", ExternalId(external_id="tr_in_workflow")),
        }
