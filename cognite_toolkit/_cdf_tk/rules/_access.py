from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import ClassVar, Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, SessionsAcl
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
    Recommendation,
)
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError, SandboxUnavailableError
from cognite_toolkit._cdf_tk.resource_ios import (
    FunctionScheduleIO,
    HostedExtractorDestinationIO,
    ResourceType,
    TransformationIO,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.time import ms_to_datetime

from ._base import (
    InternalValidatorException,
    RuleSetStatus,
    ToolkitRuleSet,
    ValidationInput,
    quote_identifier,
)

VALID_SESSION_STATUSES = frozenset({"READY", "ACTIVE"})


def retrieve_sessions(client: ToolkitClient, session_ids: set[int]) -> dict[int, SessionResponse] | None:
    """Retrieves the sessions by ID, or returns None if they cannot be verified.

    Without sessionsAcl:LIST, retrieving sessions silently returns nothing, which would wrongly mark every
    session as not found. Permissions cannot be inspected in a sandbox, so sessions are not verified there.

    Raises:
        ToolkitAPIError: If retrieving the sessions fails.
        AuthorizationError: If inspecting the permissions of the credentials fails.
    """
    if not session_ids:
        return {}
    try:
        if client.token.verify_acls([SessionsAcl(actions=["LIST"], scope=AllScope())]):
            return None
    except SandboxUnavailableError:
        return None
    sessions = client.sessions.retrieve(InternalId.from_ids(sorted(session_ids)), ignore_unknown_ids=True)
    return {session.id: session for session in sessions}


def invalid_session_status(session: SessionResponse | None) -> str | None:
    """Returns why the session is not valid, e.g. 'not found' or 'revoked', or None if it is valid."""
    if session is None:
        return "not found"
    if session.status is not None and session.status not in VALID_SESSION_STATUSES:
        return session.status.lower().replace("_", " ")
    return None


def transformation_session_ids(transformation: TransformationResponse) -> set[int]:
    return {
        session.session_id
        for session in (transformation.source_session, transformation.destination_session)
        if session is not None
    }


@dataclass
class Compute:
    """A resource that runs in CDF with credentials stored as sessions."""

    compute_type: str
    name: str
    url: str
    fix: str
    session_ids: set[int] = field(default_factory=set)

    @property
    def display_name(self) -> str:
        return f"{self.compute_type} {self.name}"


class AccessRuleSet(ToolkitRuleSet):
    """Checks the credentials of transformations, function schedules, and hosted extractor destinations in CDF.

    Credentials that are no longer valid are reported as errors, credentials that expire within
    the warning period as warnings, and credentials that expire within the recommendation period as
    recommendations.
    """

    DISPLAY_NAME: ClassVar[str] = "Access"
    RESOURCE_TYPES: ClassVar[frozenset[ResourceType]] = frozenset(
        {
            TransformationIO.as_resource_type(),
            FunctionScheduleIO.as_resource_type(),
            HostedExtractorDestinationIO.as_resource_type(),
        }
    )
    CONTEXT: ClassVar[frozenset[Literal["cdf"]]] = frozenset({"cdf"})

    def __init__(
        self,
        client: ToolkitClient | None = None,
        warning_period: timedelta = timedelta(days=3),
        recommendation_period: timedelta = timedelta(days=7),
    ) -> None:
        super().__init__(client=client)
        self.warning_period = warning_period
        self.recommendation_period = recommendation_period

    def get_status(self, data: ValidationInput) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(
                code="skip",
                message="Cannot check credentials without a ToolkitClient. Provide one to the rule set.",
            )
        return RuleSetStatus(
            code="ready",
            message=(
                "Checks for expired credentials, and credentials expiring within the next "
                f"{self.recommendation_period.days} days, in CDF."
            ),
        )

    def validate(self, data: ValidationInput) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        computes = self._load_computes(self.client)
        session_ids: set[int] = set().union(*(compute.session_ids for compute in computes))
        if not session_ids:
            return
        try:
            sessions = retrieve_sessions(self.client, session_ids)
        except (ToolkitAPIError, AuthorizationError) as e:
            yield InternalValidatorException(message=f"Failed to verify sessions in CDF: {e}", source="Session")
            return
        if sessions is None:
            yield BuildWarning(
                code="UNVERIFIED-CREDENTIALS",
                message=(
                    f"Couldn't verify the credentials of {len(computes)} transformations, function schedules, "
                    "and hosted extractor destinations."
                ),
                source_url="/token/inspect",
                fix="Add sessionsAcl:LIST to your credentials to check them.",
            )
            return
        now = datetime.now(UTC)
        for compute in computes:
            if insight := self._check_credentials(compute, sessions, now):
                yield insight

    def _load_computes(self, client: ToolkitClient) -> list[Compute]:
        computes = [
            Compute(
                compute_type="Transformation",
                name=quote_identifier(transformation.as_id()),
                url=f"/transformations/{transformation.id}/",
                fix="Update the source and destination credentials of the transformation.",
                session_ids=transformation_session_ids(transformation),
            )
            for transformation in client.transformations.list(limit=None)
        ]
        computes.extend(
            Compute(
                compute_type="Function schedule",
                name=f"'{schedule.name}' of function '{schedule.function_external_id or schedule.function_id}'",
                url=f"/functions/{schedule.function_id}/" if schedule.function_id is not None else "/functions",
                fix="Recreate the function schedule with new credentials.",
                session_ids={schedule.session_id},
            )
            for schedule in client.functions.schedules.list(limit=None)
            if schedule.session_id is not None
        )
        computes.extend(
            Compute(
                compute_type="Hosted extractor destination",
                name=quote_identifier(destination.as_id()),
                url="/hostedextractors/destinations",
                fix="Update the credentials of the hosted extractor destination.",
                session_ids={destination.session_id},
            )
            for destination in client.hosted_extractors.destinations.list(limit=None)
            if destination.session_id is not None
        )
        return computes

    def _check_credentials(
        self, compute: Compute, sessions: dict[int, SessionResponse], now: datetime
    ) -> Insight | None:
        """Reports the session of the compute that is invalid, or that expires first."""
        invalid: list[str] = []
        first_expiry: datetime | None = None
        for session_id in sorted(compute.session_ids):
            session = sessions.get(session_id)
            if status := invalid_session_status(session):
                invalid.append(f"session {session_id} is {status}")
                continue
            if session is None:
                continue
            if session.expiration_time is None:
                continue
            expires = ms_to_datetime(session.expiration_time)
            if expires <= now:
                invalid.append(f"session {session_id} expired on {expires:%Y-%m-%d %H:%M} UTC")
            elif first_expiry is None or expires < first_expiry:
                first_expiry = expires

        if invalid:
            return BuildError(
                code="INVALID-CREDENTIALS",
                message=f"{compute.display_name} has credentials that are no longer valid, as {humanize_collection(invalid)}.",
                source_url=compute.url,
                fix=compute.fix,
            )
        if first_expiry is None:
            return None
        remaining = first_expiry - now
        if remaining > self.recommendation_period:
            return None
        insight_cls = BuildWarning if remaining <= self.warning_period else Recommendation
        return insight_cls(
            code="EXPIRING-CREDENTIALS",
            message=(
                f"{compute.display_name} has credentials that expire {self._humanize_remaining(remaining)}, "
                f"on {first_expiry:%Y-%m-%d %H:%M} UTC."
            ),
            source_url=compute.url,
            fix=compute.fix,
        )

    @staticmethod
    def _humanize_remaining(remaining: timedelta) -> str:
        if remaining.days == 0:
            return "in less than a day"
        if remaining.days == 1:
            return "in 1 day"
        return f"in {remaining.days} days"
