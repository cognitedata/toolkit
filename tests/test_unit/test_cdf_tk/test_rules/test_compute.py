import time
from typing import Any
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
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
        client.sessions.retrieve.return_value = [
            SessionResponse(id=10, expiration_time=NOW - DAY),
            SessionResponse(id=11, expiration_time=NOW + DAY),
            SessionResponse(id=12, expiration_time=NOW + 5 * DAY),
            SessionResponse(id=13, expiration_time=NOW + 30 * DAY),
        ]

        insights = sorted((insight.insight_type, insight.code, insight.source_url) for insight in _validate(client))

        assert insights == [
            ("Error", "INVALID-CREDENTIALS", f"{API_URL}/transformations/1"),
            ("Recommendation", "INVALID-CREDENTIALS", f"{API_URL}/transformations/3"),
            ("Warning", "INVALID-CREDENTIALS", f"{API_URL}/transformations/2"),
        ]

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

    def test_unverified_credentials_when_sessions_cannot_be_retrieved(self) -> None:
        client = _client()
        client.transformations.list.return_value = [_transformation(1, "tr", sourceSession=_session_info(10))]
        client.sessions.retrieve.side_effect = ToolkitAPIError("Forbidden", code=403)

        codes = {insight.code for insight in _validate(client)}

        assert codes == {"UNVERIFIED-CREDENTIALS"}

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
