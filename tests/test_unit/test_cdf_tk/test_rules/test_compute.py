import time
from typing import Any
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import FunctionCallResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import FunctionScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    HostedExtractorJobLogResponse,
    HostedExtractorJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import UnknownSourceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.raw import RAWTableResponse
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import TransformationScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import WorkflowExecutionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionResponse
from cognite_toolkit._cdf_tk.commands._validation import ValidationCommand
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import InsightDefinition
from cognite_toolkit._cdf_tk.rules import ComputeRuleSet

API_URL = "https://bluefield.cognitedata.com/api/v1/projects/my_project"
NOW = int(time.time() * 1000)
DAY = 24 * 60 * 60 * 1000


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


def _session_info(session_id: int) -> dict[str, Any]:
    return {"sessionId": session_id, "projectName": "my_project"}


def _failed_job(error: str) -> dict[str, Any]:
    return {"status": "Failed", "error": error}


def _client() -> MagicMock:
    client = MagicMock()
    client.config.create_api_url.side_effect = lambda endpoint: f"{API_URL}{endpoint}"
    return client


def _validate(client: MagicMock) -> list[InsightDefinition]:
    return [insight for insight in ComputeRuleSet(client=client).validate() if isinstance(insight, InsightDefinition)]


class TestComputeRuleSet:
    def test_credentials_are_rated_by_time_to_expiry(self) -> None:
        client = _client()
        client.transformations.list.return_value = [
            _transformation(
                1, "tr_expired", sourceSession=_session_info(10), lastFinishedJob=_failed_job("Unauthorized")
            ),
            _transformation(2, "tr_expires_tomorrow", sourceSession=_session_info(11)),
            _transformation(3, "tr_expires_this_week", destinationSession=_session_info(12)),
            _transformation(4, "tr_expires_next_month", sourceSession=_session_info(13)),
        ]
        client.functions.list.return_value = [FunctionResponse(id=7, name="fn", file_id=1, created_time=0)]
        client.functions.schedules.list.return_value = [
            FunctionScheduleResponse(
                id=1, name="daily", cron_expression="0 3 * * *", created_time=0, when="", function_id=7, session_id=14
            )
        ]
        client.functions.calls.list.return_value = [
            FunctionCallResponse(id=1, status="Completed", start_time=NOW - DAY, function_id=7)
        ]
        client.sessions.retrieve.return_value = [
            SessionResponse(id=10, expiration_time=NOW - DAY),
            SessionResponse(id=11, expiration_time=NOW + DAY),
            SessionResponse(id=12, expiration_time=NOW + 5 * DAY),
            SessionResponse(id=13, expiration_time=NOW + 30 * DAY),
            SessionResponse(id=14, expiration_time=NOW - DAY),
        ]

        insights = sorted((insight.insight_type, insight.code, insight.source_url) for insight in _validate(client))

        assert insights == [
            ("Error", "EXPIRED-CREDENTIALS", f"{API_URL}/functions/7"),
            ("Error", "EXPIRED-CREDENTIALS", f"{API_URL}/transformations/1"),
            ("Recommendation", "EXPIRING-CREDENTIALS", f"{API_URL}/transformations/3"),
            ("Warning", "EXPIRING-CREDENTIALS", f"{API_URL}/transformations/2"),
        ]

    def test_congested_schedules(self) -> None:
        client = _client()
        client.transformations.list.return_value = [_transformation(id_, f"tr_{id_}") for id_ in range(1, 8)]
        client.transformations.schedules.list.return_value = [
            TransformationScheduleResponse(
                id=id_,
                external_id=f"tr_{id_}",
                interval="15 3 * * *" if id_ == 7 else "0 * * * *",
                is_paused=id_ == 6,
                created_time=0,
                last_updated_time=0,
            )
            for id_ in range(1, 8)
        ]

        insights = {(insight.code, insight.source_url, insight.message) for insight in _validate(client)}

        message = (
            "5 transformations start at the same time, every hour, on the hour: 'tr_1', 'tr_2', 'tr_3', 'tr_4' and "
            "'tr_5'. Starting many at once can exceed concurrency limits, which delays or fails runs."
        )
        assert insights == {("CONGESTED-SCHEDULE", f"{API_URL}/transformations/{id_}", message) for id_ in range(1, 6)}

    def test_failures_are_reported_with_their_likely_cause(self) -> None:
        client = _client()
        client.transformations.list.return_value = [
            _transformation(
                1,
                "tr_missing_source",
                query="SELECT * FROM src_db.gone",
                destination={"type": "raw", "database": "dst_db", "table": "table_ok"},
                lastFinishedJob=_failed_job("Table not found"),
            ),
            _transformation(2, "tr_broken_query", lastFinishedJob=_failed_job("Syntax error")),
        ]
        client.raw.tables.list.side_effect = lambda db_name, limit: (
            [RAWTableResponse(db_name="dst_db", name="table_ok", created_time=0)] if db_name == "dst_db" else []
        )
        client.workflows.versions.list.return_value = [
            WorkflowVersionResponse.model_validate(
                {
                    "workflowExternalId": "wf",
                    "version": "v1",
                    "createdTime": 0,
                    "lastUpdatedTime": 0,
                    "workflowDefinition": {
                        "tasks": [
                            {
                                "externalId": "run",
                                "type": "transformation",
                                "parameters": {"transformation": {"externalId": "tr_gone"}},
                            }
                        ]
                    },
                }
            )
        ]
        client.transformations.retrieve.return_value = []
        client.workflows.executions.list.return_value = [
            WorkflowExecutionResponse(
                id="exec1",
                workflow_external_id="wf",
                version="v1",
                status="FAILED",
                created_time=NOW - DAY,
                reason_for_incompletion="Task run failed",
            )
        ]
        client.hosted_extractors.jobs.list.return_value = [
            HostedExtractorJobResponse.model_validate(
                {
                    "externalId": "job_failing",
                    "sourceId": "src",
                    "destinationId": "dst",
                    "format": {"type": "cognite"},
                    "status": "transform_error",
                    "createdTime": 0,
                    "lastUpdatedTime": 0,
                }
            )
        ]
        client.hosted_extractors.sources.list.return_value = [
            UnknownSourceResponse(type="custom", external_id="src", created_time=0, last_updated_time=0)
        ]
        client.hosted_extractors.destinations.list.return_value = [
            HostedExtractorDestinationResponse(external_id="dst", created_time=0, last_updated_time=0)
        ]
        client.hosted_extractors.jobs.list_logs.return_value = [
            HostedExtractorJobLogResponse(
                job_external_id="job_failing", type="transform_error", created_time=NOW, message="Invalid JSON"
            )
        ]
        client.hosted_extractors.jobs.list_dlq_messages.side_effect = ToolkitAPIError("DLQ is not enabled", code=400)

        insights = sorted((insight.code, insight.source_url, insight.message) for insight in _validate(client))

        assert insights == [
            (
                "FAILED-EXECUTION",
                f"{API_URL}/hostedextractors/jobs/logs?job=job_failing",
                "The hosted extractor job 'job_failing' is failing with status 'transform_error': Invalid JSON.",
            ),
            (
                "FAILED-EXECUTION",
                f"{API_URL}/transformations/2",
                "The last run of the transformation 'tr_broken_query' failed: Syntax error.",
            ),
            (
                "MISSING-REFERENCED-RESOURCE",
                f"{API_URL}/transformations/1",
                "The last run of the transformation 'tr_missing_source' failed: Table not found. Likely cause: The "
                "transformation 'tr_missing_source' reads from the RAW table 'src_db.gone', which does not exist.",
            ),
            (
                "MISSING-REFERENCED-RESOURCE",
                f"{API_URL}/workflows/wf/versions/v1",
                "The last execution of the workflow 'wf' version 'v1' failed: Task run failed. Likely cause: The task "
                "'run' of the workflow 'wf' version 'v1' runs the transformation 'tr_gone', which does not exist.",
            ),
        ]

    def test_unverified_credentials_when_sessions_cannot_be_retrieved(self) -> None:
        client = _client()
        client.transformations.list.return_value = [_transformation(1, "tr", sourceSession=_session_info(10))]
        client.sessions.retrieve.side_effect = ToolkitAPIError("Forbidden", code=403)

        codes = {insight.code for insight in _validate(client)}

        assert codes == {"UNVERIFIED-CREDENTIALS"}

    def test_planned_in_cdf_context_without_resources(self) -> None:
        plan = ValidationCommand._create_validation_plan(client=MagicMock(), context="cdf")

        assert ComputeRuleSet in {type(step.rule) for step in plan}
