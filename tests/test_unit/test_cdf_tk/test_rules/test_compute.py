import time
from collections import Counter
from typing import Any
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import FunctionScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import HostedExtractorJobResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    CertificateResponse,
    RESTSourceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import TransformationScheduleResponse
from cognite_toolkit._cdf_tk.commands._validation import ValidationCommand
from cognite_toolkit._cdf_tk.data_classes._insights import InsightDefinition
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
            _transformation(1, "tr_expired", sourceSession=_session_info(10)),
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
        client.hosted_extractors.jobs.list.return_value = [
            HostedExtractorJobResponse.model_validate(
                {
                    "externalId": "job",
                    "sourceId": "src",
                    "destinationId": "dst",
                    "format": {"type": "cognite"},
                    "createdTime": 0,
                    "lastUpdatedTime": 0,
                }
            )
        ]
        client.hosted_extractors.destinations.list.return_value = [
            HostedExtractorDestinationResponse(external_id="dst", session_id=15, created_time=0, last_updated_time=0)
        ]
        client.sessions.retrieve.return_value = [
            SessionResponse(id=10, expiration_time=NOW - DAY),
            SessionResponse(id=11, expiration_time=NOW + DAY),
            SessionResponse(id=12, expiration_time=NOW + 5 * DAY),
            SessionResponse(id=13, expiration_time=NOW + 30 * DAY),
            SessionResponse(id=14, expiration_time=NOW - DAY),
            SessionResponse(id=15, expiration_time=NOW + DAY),
        ]

        insights = sorted((insight.insight_type, insight.code, insight.source_url) for insight in _validate(client))

        assert insights == [
            ("Error", "INVALID-CREDENTIALS", f"{API_URL}/functions/7"),
            ("Error", "INVALID-CREDENTIALS", f"{API_URL}/transformations/1"),
            ("Recommendation", "EXPIRING-CREDENTIALS", f"{API_URL}/transformations/3"),
            ("Warning", "EXPIRING-CREDENTIALS", f"{API_URL}/hostedextractors/jobs/logs?job=job"),
            ("Warning", "EXPIRING-CREDENTIALS", f"{API_URL}/transformations/2"),
        ]

    def test_expiry_message_includes_session_status_when_available(self) -> None:
        client = _client()
        client.transformations.list.return_value = [
            _transformation(1, "tr_with_status", sourceSession=_session_info(10)),
            _transformation(2, "tr_without_status", sourceSession=_session_info(11)),
        ]
        client.sessions.retrieve.return_value = [
            SessionResponse(id=10, status="ACTIVE", expiration_time=NOW + DAY),
            SessionResponse(id=11, expiration_time=NOW + DAY),
        ]

        with_status, without_status = _validate(client)

        assert with_status.message.endswith("The session 10 is active.")
        assert "The session" not in without_status.message

    def test_credentials_are_invalid_when_session_status_is_not_valid(self) -> None:
        client = _client()
        client.transformations.list.return_value = [
            _transformation(1, "tr_ready", sourceSession=_session_info(10)),
            _transformation(2, "tr_active", sourceSession=_session_info(11)),
            _transformation(3, "tr_revoked", sourceSession=_session_info(12)),
            _transformation(4, "tr_unknown_status", sourceSession=_session_info(13)),
            _transformation(5, "tr_expired_and_status_expired", destinationSession=_session_info(14)),
        ]
        client.sessions.retrieve.return_value = [
            SessionResponse(id=10, status="READY", expiration_time=NOW + 30 * DAY),
            SessionResponse(id=11, status="ACTIVE", expiration_time=NOW + 30 * DAY),
            SessionResponse(id=12, status="REVOKED", expiration_time=NOW + 30 * DAY),
            SessionResponse(id=13),
            SessionResponse(id=14, status="EXPIRED", expiration_time=NOW - DAY),
        ]

        results = _validate(client)
        insights = [(insight.insight_type, insight.code, insight.source_url) for insight in results]

        # A credential is reported at most once, even if both its status and expiry are invalid.
        assert insights == [
            ("Error", "INVALID-CREDENTIALS", f"{API_URL}/transformations/3"),
            ("Error", "INVALID-CREDENTIALS", f"{API_URL}/transformations/5"),
        ]
        assert "session 12 is revoked" in results[0].message

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

    def test_overlapping_congested_schedules_warn_once_per_resource(self) -> None:
        client = _client()
        client.transformations.list.return_value = [_transformation(id_, f"tr_{id_}") for id_ in range(1, 7)]
        client.transformations.schedules.list.return_value = [
            TransformationScheduleResponse(
                id=id_,
                external_id=f"tr_{id_}",
                interval="0 0 * * *" if id_ == 6 else "0 * * * *",
                is_paused=False,
                created_time=0,
                last_updated_time=0,
            )
            for id_ in range(1, 7)
        ]

        warning_count_by_url = Counter(insight.source_url for insight in _validate(client))

        assert warning_count_by_url == {f"{API_URL}/transformations/{id_}": 1 for id_ in range(1, 7)}

    def test_unverified_credentials_when_sessions_cannot_be_retrieved(self) -> None:
        client = _client()
        client.transformations.list.return_value = [_transformation(1, "tr", sourceSession=_session_info(10))]
        client.sessions.retrieve.side_effect = ToolkitAPIError("Forbidden", code=403)

        codes = {insight.code for insight in _validate(client)}

        assert codes == {"UNVERIFIED-CREDENTIALS"}

    def test_certificates_are_checked_when_sessions_cannot_be_retrieved(self) -> None:
        client = _client()
        client.transformations.list.return_value = [_transformation(1, "tr", sourceSession=_session_info(10))]
        client.hosted_extractors.jobs.list.return_value = [
            HostedExtractorJobResponse.model_validate(
                {
                    "externalId": "job",
                    "sourceId": "src",
                    "destinationId": "dst",
                    "format": {"type": "cognite"},
                    "createdTime": 0,
                    "lastUpdatedTime": 0,
                }
            )
        ]
        client.hosted_extractors.sources.list.return_value = [
            RESTSourceResponse(
                external_id="src",
                host="example.com",
                ca_certificate=CertificateResponse(thumbprint="abc", expires_at=NOW - DAY),
                created_time=0,
                last_updated_time=0,
            )
        ]
        client.sessions.retrieve.side_effect = ToolkitAPIError("Forbidden", code=403)

        insights = sorted((insight.insight_type, insight.code, insight.source_url) for insight in _validate(client))

        assert insights == [
            ("Error", "INVALID-CREDENTIALS", f"{API_URL}/hostedextractors/jobs/logs?job=job"),
            ("Warning", "UNVERIFIED-CREDENTIALS", f"{API_URL}/sessions/byids"),
        ]

    def test_missing_session_is_reported_as_expired(self) -> None:
        client = _client()
        client.transformations.list.return_value = [
            _transformation(1, "tr_valid", sourceSession=_session_info(10)),
            _transformation(2, "tr_deleted_session", sourceSession=_session_info(11)),
        ]
        client.sessions.retrieve.return_value = [SessionResponse(id=10, expiration_time=NOW + 30 * DAY)]

        insights = [(insight.insight_type, insight.code, insight.source_url) for insight in _validate(client)]

        assert insights == [("Error", "INVALID-CREDENTIALS", f"{API_URL}/transformations/2")]

    def test_planned_in_cdf_context_without_resources(self) -> None:
        plan = ValidationCommand._create_validation_plan(client=MagicMock(), context="cdf")

        assert ComputeRuleSet in {type(step.rule) for step in plan}
