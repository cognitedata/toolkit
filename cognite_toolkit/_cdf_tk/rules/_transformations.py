from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from typing import Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import InternalId
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import SessionInfo, TransformationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import TransformationScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    SubworkflowRef,
    SubworkflowTaskParameters,
    Task,
    TransformationRef,
    TransformationTaskParameters,
    WorkflowVersionResponse,
)
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import TransformationIO
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitRuleSet

_INVALID_SESSION_STATUSES = frozenset({"CANCELLED", "EXPIRED", "REVOKED", "ACCESS_LOST"})
_SESSIONS_ENDPOINT = "/sessions/byids"

CredentialKind = Literal["source", "destination"]


def _transformation_refs(tasks: Sequence[Task]) -> Iterable[TransformationRef]:
    """The transformations referenced by the tasks, including the tasks of inline subworkflows."""
    for task in tasks:
        parameters = task.parameters
        if isinstance(parameters, TransformationTaskParameters):
            yield parameters.transformation
        elif isinstance(parameters, SubworkflowTaskParameters) and isinstance(parameters.subworkflow, SubworkflowRef):
            yield from _transformation_refs(parameters.subworkflow.tasks)


def _workflow_refs_by_transformation(
    workflow_versions: Sequence[WorkflowVersionResponse],
) -> dict[str, list[TransformationRef]]:
    refs_by_transformation: dict[str, list[TransformationRef]] = defaultdict(list)
    for workflow_version in workflow_versions:
        for ref in _transformation_refs(workflow_version.workflow_definition.tasks):
            refs_by_transformation[ref.external_id].append(ref)
    return refs_by_transformation


class TransformationRuleSet(ToolkitRuleSet):
    """Validates the transformations in CDF.

    Checks that transformations are not blocked, that they are triggered by a schedule or a workflow, and that
    the credentials they run with are set and still valid.
    """

    RESOURCE_TYPES = frozenset({TransformationIO.as_resource_type()})
    CONTEXT = frozenset({"cdf"})
    DISPLAY_NAME = "Transformations"

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(code="skip", message="Transformation validation requires a client to read CDF.")
        return RuleSetStatus(
            code="ready",
            message="Will validate that transformations in CDF are triggered, not blocked, and have valid credentials.",
        )

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        try:
            transformations = self.client.transformations.list(limit=None)
            schedules = self.client.transformations.schedules.list(limit=None)
            workflow_versions = self.client.workflows.versions.list(limit=None)
        except ToolkitAPIError as error:
            yield InternalValidatorException(
                message=f"Failed to retrieve transformations, schedules, or workflow versions: {error}",
                source=self.DISPLAY_NAME,
            )
            return
        schedule_by_transformation = {schedule.external_id: schedule for schedule in schedules}
        workflow_refs_by_transformation = _workflow_refs_by_transformation(workflow_versions)
        sessions_by_id, unverified = self._retrieve_sessions(self.client, transformations)
        if unverified:
            yield unverified

        for transformation in transformations:
            source_url = self.client.config.create_api_url(f"/transformations/{transformation.id}")
            schedule = schedule_by_transformation.get(transformation.external_id)
            workflow_refs = workflow_refs_by_transformation.get(transformation.external_id, [])
            yield from self._check_blocked(transformation, source_url)
            yield from self._check_triggered(transformation, schedule, workflow_refs, source_url)
            yield from self._check_credentials(transformation, schedule, workflow_refs, sessions_by_id, source_url)

    @staticmethod
    def _retrieve_sessions(
        client: ToolkitClient, transformations: Sequence[TransformationResponse]
    ) -> tuple[dict[int, SessionResponse], BuildWarning | None]:
        """The sessions the transformations run with, and a warning if their status could not be verified."""
        session_ids = {
            session.session_id
            for transformation in transformations
            for session in (transformation.source_session, transformation.destination_session)
            if session is not None
        }
        if not session_ids:
            return {}, None
        fix = "Ensure that the provided token has LIST access to sessions (sessionsAcl:LIST)."
        source_url = client.config.create_api_url(_SESSIONS_ENDPOINT)
        try:
            sessions = client.sessions.retrieve(
                [InternalId(id=session_id) for session_id in sorted(session_ids)], ignore_unknown_ids=True
            )
        except ToolkitAPIError as error:
            return {}, BuildWarning(
                code="UNVERIFIED-CREDENTIALS",
                message=f"Could not verify the credentials of transformations, failed to retrieve their sessions: {error}",
                fix=fix,
                source_url=source_url,
                alpha=Flags.ALPHA_RULES.is_enabled(),
            )
        sessions_by_id = {session.id: session for session in sessions if session.status is not None}
        if len(sessions_by_id) == len(sessions):
            return sessions_by_id, None
        return sessions_by_id, BuildWarning(
            code="UNVERIFIED-CREDENTIALS",
            message="Could not verify the credentials of all transformations, the status of their sessions is unknown.",
            fix=fix,
            source_url=source_url,
            alpha=Flags.ALPHA_RULES.is_enabled(),
        )

    @staticmethod
    def _check_blocked(transformation: TransformationResponse, source_url: str) -> Iterable[Insight]:
        if transformation.blocked is None:
            return
        yield BuildError(
            code="BLOCKED-RESOURCE",
            message=(
                f"The transformation '{transformation.external_id}' is blocked and cannot run. "
                f"Reason: {transformation.blocked.reason}"
            ),
            fix="Resolve the reason the transformation was blocked, then ask Cognite support to unblock it.",
            source_url=source_url,
            alpha=Flags.ALPHA_RULES.is_enabled(),
        )

    @staticmethod
    def _check_triggered(
        transformation: TransformationResponse,
        schedule: TransformationScheduleResponse | None,
        workflow_refs: Sequence[TransformationRef],
        source_url: str,
    ) -> Iterable[Insight]:
        if workflow_refs or (schedule is not None and not schedule.is_paused):
            return
        reason = "its schedule is paused" if schedule is not None else "it has no schedule"
        yield BuildWarning(
            code="UNTRIGGERED-RESOURCE",
            message=(
                f"The transformation '{transformation.external_id}' never runs automatically: "
                f"{reason} and no workflow runs it."
            ),
            fix="Add a schedule to the transformation or a task that runs it to a workflow, or delete it if unused.",
            source_url=source_url,
            alpha=Flags.ALPHA_RULES.is_enabled(),
        )

    @classmethod
    def _check_credentials(
        cls,
        transformation: TransformationResponse,
        schedule: TransformationScheduleResponse | None,
        workflow_refs: Sequence[TransformationRef],
        sessions_by_id: Mapping[int, SessionResponse],
        source_url: str,
    ) -> Iterable[Insight]:
        credentials: list[tuple[CredentialKind, SessionInfo | None, bool]] = [
            ("source", transformation.source_session, transformation.has_source_oidc_credentials),
            ("destination", transformation.destination_session, transformation.has_destination_oidc_credentials),
        ]
        # Workflows run transformations with the workflow's own credentials unless told otherwise.
        runs_with_own_credentials = schedule is not None or any(
            ref.use_transformation_credentials for ref in workflow_refs
        )
        for kind, session, has_oidc_credentials in credentials:
            if session is None and not has_oidc_credentials:
                if runs_with_own_credentials:
                    yield cls._missing_credentials(transformation, kind, source_url)
                continue
            if session is None or (cdf_session := sessions_by_id.get(session.session_id)) is None:
                continue
            if cdf_session.status in _INVALID_SESSION_STATUSES:
                yield BuildError(
                    code="INVALID-CREDENTIALS",
                    message=(
                        f"The {kind} credentials of the transformation '{transformation.external_id}' are no longer "
                        f"valid. Its session {session.session_id} has status {cdf_session.status}."
                    ),
                    fix=(
                        f"Set new {kind} credentials on the transformation, for example, by deploying it again "
                        "with Toolkit."
                    ),
                    source_url=source_url,
                    alpha=Flags.ALPHA_RULES.is_enabled(),
                )

    @staticmethod
    def _missing_credentials(
        transformation: TransformationResponse, kind: CredentialKind, source_url: str
    ) -> BuildError:
        return BuildError(
            code="MISSING-CREDENTIALS",
            message=(
                f"The transformation '{transformation.external_id}' has no {kind} credentials, but is run by a "
                "schedule or by a workflow that uses the transformation's credentials. These runs fail."
            ),
            fix=f"Set {kind} credentials on the transformation, for example, with 'authentication' in its YAML file.",
            source_url=source_url,
            alpha=Flags.ALPHA_RULES.is_enabled(),
        )
