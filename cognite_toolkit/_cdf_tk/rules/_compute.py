import time
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
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
    """Credentials a compute resource runs with. They expire when either the session or the certificate does."""

    url: str
    name: str
    fix: str
    session_id: int | None = None
    expires_at: int | None = None


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

    Finds credentials that expire soon, or have already expired.
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
        session_ids = sorted({credential.session_id for credential in credentials if credential.session_id})
        expiry_by_session_id, unverified = self._session_expiry(client, session_ids)
        if unverified:
            yield unverified
        for credential in credentials:
            if credential.session_id is not None:
                expires_at = expiry_by_session_id.get(credential.session_id)
                if expires_at is None and unverified is None:
                    yield _Finding(
                        url=credential.url,
                        severity="error",
                        code="EXPIRED-CREDENTIALS",
                        message=f"{credential.name} are invalid, their session {credential.session_id} was not found.",
                        fix=credential.fix,
                    )
                    continue
            else:
                expires_at = credential.expires_at
            if expires_at is not None and (finding := self._expiry_finding(credential, expires_at, now)):
                yield finding

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
    def _session_expiry(client: ToolkitClient, session_ids: Sequence[int]) -> tuple[dict[int, int], _Finding | None]:
        """The expiry of the sessions by ID, and a finding if the expiry of some could not be verified."""
        if not session_ids:
            return {}, None
        message = "Could not verify when the credentials of all compute resources expire."
        try:
            sessions = client.sessions.retrieve(
                [InternalId(id=session_id) for session_id in session_ids], ignore_unknown_ids=True
            )
        except ToolkitAPIError as error:
            sessions = None
            message = f"{message} Failed to retrieve their sessions: {error}"
        expiry_by_session_id = {
            session.id: session.expiration_time for session in sessions or [] if session.expiration_time is not None
        }
        if sessions is not None and len(expiry_by_session_id) == len(sessions):
            return expiry_by_session_id, None
        return expiry_by_session_id, _Finding(
            url=client.config.create_api_url(_SESSIONS_ENDPOINT),
            severity="warning",
            code="UNVERIFIED-CREDENTIALS",
            message=message,
            fix="Ensure that the provided token has LIST access to sessions (sessionsAcl:LIST).",
        )

    @staticmethod
    def _expiry_finding(credential: _Credential, expires_at: int, now: int) -> _Finding | None:
        remaining = expires_at - now
        expiry = ms_to_datetime(expires_at).strftime("%Y-%m-%d %H:%M UTC")
        if remaining <= 0:
            return _Finding(
                url=credential.url,
                severity="error",
                code="EXPIRED-CREDENTIALS",
                message=f"{credential.name} expired {_humanize_duration(-remaining)} ago, on {expiry}.",
                fix=credential.fix,
            )
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
            code="EXPIRING-CREDENTIALS",
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
