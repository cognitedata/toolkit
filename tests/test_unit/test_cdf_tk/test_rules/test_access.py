from datetime import UTC, datetime, timedelta
from typing import Any
from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import FunctionScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, SessionsAcl
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
from cognite_toolkit._cdf_tk.rules import AccessRuleSet
from cognite_toolkit._cdf_tk.rules._base import ValidationInput
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms

NOW = datetime.now(UTC)


def _in(days: float) -> int:
    return datetime_to_ms(NOW + timedelta(days=days))


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


def _destination(external_id: str, session_id: int) -> HostedExtractorDestinationResponse:
    return HostedExtractorDestinationResponse(
        external_id=external_id, session_id=session_id, created_time=0, last_updated_time=0
    )


def _client() -> MagicMock:
    client = MagicMock()
    client.token.verify_acls.return_value = []
    client.transformations.list.return_value = [
        _transformation(
            1,
            "tr_expired",
            sourceSession={"sessionId": 100, "projectName": "my_project"},
            destinationSession={"sessionId": 101, "projectName": "my_project"},
        ),
        _transformation(2, "tr_healthy", sourceSession={"sessionId": 102, "projectName": "my_project"}),
        _transformation(3, "tr_without_credentials"),
    ]
    client.functions.schedules.list.return_value = [
        FunctionScheduleResponse(
            id=7,
            name="hourly",
            cron_expression="0 * * * *",
            created_time=0,
            when="hourly",
            function_id=4,
            function_external_id="my_function",
            session_id=200,
        )
    ]
    client.hosted_extractors.destinations.list.return_value = [
        _destination("dest_soon", 300),
        _destination("dest_revoked", 301),
        _destination("dest_missing", 302),
    ]
    client.sessions.retrieve.return_value = [
        SessionResponse(id=100, status="ACTIVE", expiration_time=_in(30)),
        SessionResponse(id=101, status="ACTIVE", expiration_time=_in(-1)),
        SessionResponse(id=102, status="ACTIVE", expiration_time=_in(30)),
        SessionResponse(id=200, status="ACTIVE", expiration_time=_in(2.5)),
        SessionResponse(id=300, status="READY", expiration_time=_in(5.5)),
        SessionResponse(id=301, status="REVOKED", expiration_time=_in(30)),
    ]
    return client


class TestAccessRuleSet:
    def test_reports_expired_and_expiring_credentials(self) -> None:
        insights = list(AccessRuleSet(client=_client()).validate(ValidationInput()))

        assert [(type(insight).__name__, insight.code, insight.message.split(" has ")[0]) for insight in insights] == [
            ("BuildError", "INVALID-CREDENTIALS", "Transformation 'tr_expired'"),
            ("BuildWarning", "EXPIRING-CREDENTIALS", "Function schedule 'hourly' of function 'my_function'"),
            ("Recommendation", "EXPIRING-CREDENTIALS", "Hosted extractor destination 'dest_soon'"),
            ("BuildError", "INVALID-CREDENTIALS", "Hosted extractor destination 'dest_revoked'"),
            ("BuildError", "INVALID-CREDENTIALS", "Hosted extractor destination 'dest_missing'"),
        ]

    def test_without_sessions_acl_reports_unverified_credentials(self) -> None:
        client = _client()
        client.token.verify_acls.return_value = [SessionsAcl(actions=["LIST"], scope=AllScope())]

        insights = list(AccessRuleSet(client=client).validate(ValidationInput()))

        assert ([insight.code for insight in insights], client.sessions.retrieve.called) == (
            ["UNVERIFIED-CREDENTIALS"],
            False,
        )
