import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse, SessionStatus
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
    InsightCode,
    InternalValidatorException,
    Recommendation,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import TransformationIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitRuleSet
from cognite_toolkit._cdf_tk.utils.time import ms_to_datetime

# Credentials expiring within these number of days are recommended, respectively warned, to be rotated.
ROTATE_RECOMMENDATION_DAYS = 7
ROTATE_WARNING_DAYS = 2

_DAY_MS = 24 * 60 * 60 * 1000
_HOUR_MS = 60 * 60 * 1000
_SESSIONS_ENDPOINT = "/sessions/byids"
_VALID_SESSION_STATUSES: frozenset[SessionStatus] = frozenset({"READY", "ACTIVE"})

_Severity = Literal["error", "warning", "recommendation"]
_INSIGHT_BY_SEVERITY: dict[_Severity, type[BuildError] | type[BuildWarning] | type[Recommendation]] = {
    "error": BuildError,
    "warning": BuildWarning,
    "recommendation": Recommendation,
}


@dataclass
class _Inventory:
    """The compute resources in CDF, together with the resources that decide when and how they run."""

    transformations: list[TransformationResponse] = field(default_factory=list)


@dataclass(frozen=True)
class _Credential:
    """Credentials a compute resource runs with, backed by a session."""

    url: str
    name: str
    fix: str
    session_id: int


@dataclass(frozen=True)
class _Finding:
    """A potential issue with a compute resource."""

    url: str
    severity: _Severity
    code: InsightCode
    message: str
    fix: str


class ComputeRuleSet(ToolkitRuleSet):
    """Validates the compute resources in CDF.

    Finds credentials whose session is missing or no longer valid, has expired, or expires soon.
    """

    RESOURCE_TYPES = frozenset({TransformationIO.as_resource_type()})
    CONTEXT = frozenset({"cdf"})
    DISPLAY_NAME = "Compute"

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(code="skip", message="Compute validation requires a client to read CDF.")
        return RuleSetStatus(code="ready", message="Will validate the credentials of transformations in CDF.")

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        client = self.client
        now = int(time.time() * 1000)
        inventory, exceptions = self._load_inventory(client)
        yield from exceptions

        findings: list[_Finding] = []
        for item in self._check_credentials(client, inventory, now):
            if isinstance(item, InternalValidatorException):
                yield item
            else:
                findings.append(item)

        yield from self._report(findings)

    def _load_inventory(self, client: ToolkitClient) -> tuple[_Inventory, list[InternalValidatorException]]:
        inventory = _Inventory()
        exceptions: list[InternalValidatorException] = []
        try:
            inventory.transformations = client.transformations.list(limit=None)
        except ToolkitAPIError as error:
            exceptions.append(self._retrieve_failed("transformations", error))
        return inventory, exceptions

    def _retrieve_failed(self, what: str, error: ToolkitAPIError) -> InternalValidatorException:
        return InternalValidatorException(message=f"Failed to retrieve {what}: {error}", source=self.DISPLAY_NAME)

    # 1. Credentials
    def _check_credentials(
        self, client: ToolkitClient, inventory: _Inventory, now: int
    ) -> Iterable[_Finding | InternalValidatorException]:
        credentials = self._credentials(client, inventory)
        session_ids = sorted({credential.session_id for credential in credentials})
        session_by_id, unverified = self._find_sessions(client, session_ids)
        if unverified:
            yield unverified
            return
        for credential in credentials:
            session = session_by_id.get(credential.session_id)
            if session is None:
                yield self._invalid_finding(credential, f"the session {credential.session_id} was not found.")
            elif session.status is not None and session.status not in _VALID_SESSION_STATUSES:
                yield self._invalid_finding(
                    credential, f"the session {credential.session_id} is {session.status.casefold()}."
                )
            elif session.expiration_time is not None and (
                finding := self._expiry_finding(credential, session.expiration_time, now)
            ):
                yield finding

    @staticmethod
    def _invalid_finding(credential: _Credential, reason: str) -> _Finding:
        return _Finding(
            url=credential.url,
            severity="error",
            code="INVALID-CREDENTIALS",
            message=f"{credential.name} are invalid, {reason}",
            fix=credential.fix,
        )

    @staticmethod
    def _credentials(client: ToolkitClient, inventory: _Inventory) -> list[_Credential]:
        credentials: list[_Credential] = []
        for transformation in inventory.transformations:
            for kind, session in (
                ("source", transformation.source_session),
                ("destination", transformation.destination_session),
            ):
                if session is None:
                    continue
                credentials.append(
                    _Credential(
                        url=_transformation_url(client, transformation),
                        name=f"The {kind} credentials of the transformation '{transformation.external_id}'",
                        fix=f"Rotate the {kind} credentials of the transformation, for example, by deploying it "
                        "again with Toolkit.",
                        session_id=session.session_id,
                    )
                )
        return credentials

    @staticmethod
    def _find_sessions(
        client: ToolkitClient, session_ids: Sequence[int]
    ) -> tuple[dict[int, SessionResponse], _Finding | None]:
        """The sessions by ID, and a finding if they could not be retrieved."""
        if not session_ids:
            return {}, None
        message = "Could not verify when the credentials of all compute resources expire."
        try:
            sessions = client.sessions.retrieve(
                [InternalId(id=session_id) for session_id in session_ids], ignore_unknown_ids=True
            )
        except ToolkitAPIError as error:
            message = f"{message} Failed to retrieve {len(session_ids)} sessions: {error}"
            return {}, _Finding(
                url=client.config.create_api_url(_SESSIONS_ENDPOINT),
                severity="warning",
                code="UNVERIFIED-CREDENTIALS",
                message=message,
                fix="Ensure that the provided token has LIST access to sessions (sessionsAcl:LIST).",
            )
        return {session.id: session for session in sessions}, None

    @classmethod
    def _expiry_finding(cls, credential: _Credential, expires_at: int, now: int) -> _Finding | None:
        remaining = expires_at - now
        expiry = ms_to_datetime(expires_at).strftime("%Y-%m-%d %H:%M UTC")
        if remaining <= 0:
            return cls._invalid_finding(credential, f"they expired {_humanize_duration(-remaining)} ago, on {expiry}.")
        severity: _Severity
        if remaining < ROTATE_WARNING_DAYS * _DAY_MS:
            severity = "warning"
        elif remaining < ROTATE_RECOMMENDATION_DAYS * _DAY_MS:
            severity = "recommendation"
        else:
            return None
        return _Finding(
            url=credential.url,
            severity=severity,
            code="INVALID-CREDENTIALS",
            message=f"{credential.name} will expire in {_humanize_duration(remaining)}, on {expiry}.",
            fix=credential.fix,
        )

    # 2. Report

    @staticmethod
    def _report(findings: Sequence[_Finding]) -> Iterable[Insight]:
        alpha = Flags.ALPHA_RULES.is_enabled()
        for finding in findings:
            yield _INSIGHT_BY_SEVERITY[finding.severity](
                code=finding.code, message=finding.message, fix=finding.fix, source_url=finding.url, alpha=alpha
            )


def _transformation_url(client: ToolkitClient, transformation: TransformationResponse) -> str:
    return client.config.create_api_url(f"/transformations/{transformation.id}")


def _humanize_duration(ms: int) -> str:
    if ms >= _DAY_MS:
        days = ms // _DAY_MS
        return f"{days} day{'s' if days > 1 else ''}"
    if ms >= _HOUR_MS:
        hours = ms // _HOUR_MS
        return f"{hours} hour{'s' if hours > 1 else ''}"
    return "less than an hour"
