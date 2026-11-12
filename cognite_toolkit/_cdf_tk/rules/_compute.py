import importlib.util
import time
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any, Literal
from urllib.parse import quote

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId, RawTableId, WorkflowVersionId
from cognite_toolkit._cdf_tk.client.request_classes.filters import EpochTimestampRange, FunctionCallFilter
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import FunctionScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import HostedExtractorJobResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    HostedExtractorSourceResponseUnion,
    KafkaSourceResponse,
    MQTTSourceResponse,
    RESTSourceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.session import SessionResponse, SessionStatus
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import TransformationScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import WorkflowExecutionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    ScheduleTriggerRule,
    WorkflowTriggerResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionResponse
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
    InsightCode,
    InternalValidatorException,
    Recommendation,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    DataModelIO,
    DataSetsIO,
    ExternalDataSourceIO,
    FunctionIO,
    FunctionScheduleIO,
    HostedExtractorDestinationIO,
    HostedExtractorJobIO,
    HostedExtractorMappingIO,
    HostedExtractorSourceIO,
    RawDatabaseIO,
    RawTableIO,
    ResourceIO,
    SequenceIO,
    SimulatorRoutineIO,
    SpaceIO,
    StreamIO,
    TransformationIO,
    TransformationScheduleIO,
    ViewIO,
    WorkflowIO,
    WorkflowTriggerIO,
    WorkflowVersionIO,
)
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitRuleSet, quote_identifier
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.cdf import get_transformation_sources
from cognite_toolkit._cdf_tk.utils.cron import MINUTES_PER_DAY, cron_minutes_of_week, utc_offset_minutes
from cognite_toolkit._cdf_tk.utils.time import ms_to_datetime

# Credentials expiring within these number of days are recommended, respectively warned, to be rotated.
ROTATE_RECOMMENDATION_DAYS = 7
ROTATE_WARNING_DAYS = 2
# Warn when this many compute resources of the same type are scheduled to start in the same minute.
THUNDERING_HERD_THRESHOLD = 5
# How far back to look for the last call of a function and the last execution of a workflow.
RUN_HISTORY_DAYS = 7

_DAY_MS = 24 * 60 * 60 * 1000
_HOUR_MS = 60 * 60 * 1000
_MAX_ERROR_LENGTH = 500
_MAX_LISTED_NAMES = 10
_WEEKDAY_NAMES = ("Mondays", "Tuesdays", "Wednesdays", "Thursdays", "Fridays", "Saturdays", "Sundays")
_SESSIONS_ENDPOINT = "/sessions/byids"
_VALID_SESSION_STATUSES: frozenset[SessionStatus] = frozenset({"READY", "ACTIVE"})

_FAILED_FUNCTION_CALLS = {
    "Failed": "failed",
    "Timeout": "timed out",
    "ConcurrencyLimitExceeded": "was rejected because the concurrency limit of the function was exceeded",
}
_FAILED_WORKFLOW_EXECUTIONS = {"FAILED": "failed", "TIMED_OUT": "timed out"}
_FAILED_TASK_STATUSES = frozenset({"FAILED", "FAILED_WITH_TERMINAL_ERROR", "TIMED_OUT"})
_FAILED_HOSTED_EXTRACTOR_JOBS = {
    "startup_error": "Fix the configuration of the job, its source, or its destination, then restart the job.",
    "connection_error": "Ensure that the source system is reachable and that the credentials of the source are valid.",
    "transform_error": "Update the format or mapping of the job to match the messages from the source.",
    "cdf_write_error": "Ensure that the credentials of the destination can write to the target resources in CDF.",
}

_Severity = Literal["error", "warning", "recommendation"]
_INSIGHT_BY_SEVERITY: dict[_Severity, type[BuildError] | type[BuildWarning] | type[Recommendation]] = {
    "error": BuildError,
    "warning": BuildWarning,
    "recommendation": Recommendation,
}
_VERB_AND_LABEL_BY_DEPENDENCY: dict[type[ResourceIO], tuple[str, str]] = {
    DataModelIO: ("writes to", "data model"),
    DataSetsIO: ("belongs to", "data set"),
    ExternalDataSourceIO: ("reads from", "external data source"),
    FunctionIO: ("calls", "function"),
    HostedExtractorDestinationIO: ("writes to", "hosted extractor destination"),
    HostedExtractorMappingIO: ("uses", "hosted extractor mapping"),
    HostedExtractorSourceIO: ("reads from", "hosted extractor source"),
    RawDatabaseIO: ("writes to", "RAW database"),
    RawTableIO: ("writes to", "RAW table"),
    SequenceIO: ("writes to", "sequence"),
    SimulatorRoutineIO: ("runs", "simulator routine"),
    SpaceIO: ("writes to", "instance space"),
    StreamIO: ("listens to", "stream"),
    TransformationIO: ("runs", "transformation"),
    ViewIO: ("writes to", "view"),
    WorkflowIO: ("uses", "workflow"),
    WorkflowVersionIO: ("runs", "workflow version"),
}
# Required by the YAML format, but never returned by CDF. Only used to read the dependencies.
_NOT_RETURNED_BY_CDF: dict[type[ResourceIO], dict[str, Any]] = {
    WorkflowTriggerIO: {"authentication": {"clientId": "<not-returned>", "clientSecret": "<not-returned>"}},
}


@dataclass
class _Inventory:
    """The compute resources in CDF, together with the resources that decide when and how they run."""

    transformations: list[TransformationResponse] = field(default_factory=list)
    transformation_schedules: list[TransformationScheduleResponse] = field(default_factory=list)
    functions: list[FunctionResponse] = field(default_factory=list)
    function_schedules: list[FunctionScheduleResponse] = field(default_factory=list)
    workflow_versions: list[WorkflowVersionResponse] = field(default_factory=list)
    workflow_triggers: list[WorkflowTriggerResponse] = field(default_factory=list)
    hosted_extractor_jobs: list[HostedExtractorJobResponse] = field(default_factory=list)
    hosted_extractor_sources: list[HostedExtractorSourceResponseUnion] = field(default_factory=list)
    hosted_extractor_destinations: list[HostedExtractorDestinationResponse] = field(default_factory=list)


@dataclass(frozen=True)
class _Credential:
    """Credentials a compute resource runs with, backed by a session."""

    url: str
    name: str
    fix: str
    session_id: int


@dataclass(frozen=True)
class _Certificate:
    """A certificate a compute resource connects with."""

    url: str
    name: str
    fix: str
    expires_at: int


@dataclass(frozen=True)
class _Schedule:
    url: str
    name: str
    cron_expression: str
    timezone: str | None = None


@dataclass(frozen=True)
class _Reference:
    """A resource a compute resource depends on, for example, the RAW table a transformation reads from."""

    url: str
    referrer: str
    verb: str
    label: str
    crud: type[ResourceIO]
    identifier: Identifier


@dataclass(frozen=True)
class _Failure:
    """The last run of a compute resource failed."""

    url: str
    message: str
    fix: str
    concurrency_limited: bool = False


@dataclass(frozen=True)
class _Finding:
    """A potential issue with a compute resource.

    Args:
        explains: The failures this finding can cause, any failure or only failures from exceeding a concurrency limit.
    """

    url: str
    severity: _Severity
    code: InsightCode
    message: str
    fix: str
    explains: Literal["any", "concurrency"] | None = None

    def explains_failure(self, failure: _Failure) -> bool:
        return self.explains == "any" or (self.explains == "concurrency" and failure.concurrency_limited)


class ComputeRuleSet(ToolkitRuleSet):
    """Validates the compute resources in CDF: transformations, functions, workflows, and hosted extractor jobs.

    First, finds potential issues: credentials whose session is missing or no longer valid, has expired, or expires
    soon, certificates that have expired or expire soon, many resources scheduled to start at the same time, and
    resources they depend on that do not exist. Then, reports the resources whose last run failed, with the potential
    issue as the cause if one explains the failure. The remaining potential issues are reported as is.
    """

    RESOURCE_TYPES = frozenset(
        {
            TransformationIO.as_resource_type(),
            TransformationScheduleIO.as_resource_type(),
            FunctionIO.as_resource_type(),
            FunctionScheduleIO.as_resource_type(),
            WorkflowIO.as_resource_type(),
            WorkflowVersionIO.as_resource_type(),
            WorkflowTriggerIO.as_resource_type(),
            HostedExtractorJobIO.as_resource_type(),
            HostedExtractorSourceIO.as_resource_type(),
            HostedExtractorDestinationIO.as_resource_type(),
        }
    )
    CONTEXT = frozenset({"cdf"})
    DISPLAY_NAME = "Compute"

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(code="skip", message="Compute validation requires a client to read CDF.")
        if not self._has_sql_parser():
            return RuleSetStatus(
                code="reduced",
                message=(
                    "Will not validate the sources of transformations, as sqlparse is not installed. "
                    "Install with 'pip install \"cognite-toolkit[sql]\"'."
                ),
            )
        return RuleSetStatus(
            code="ready",
            message=(
                "Will validate the credentials, schedules, dependencies, and last runs of transformations, "
                "functions, workflows, and hosted extractor jobs in CDF."
            ),
        )

    @staticmethod
    def _has_sql_parser() -> bool:
        return importlib.util.find_spec("sqlparse") is not None

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        client = self.client
        now = int(time.time() * 1000)
        inventory, exceptions = self._load_inventory(client)
        yield from exceptions

        findings_by_url: dict[str, list[_Finding]] = defaultdict(list)
        potential_issues = (
            *self._check_credentials(client, inventory, now),
            *self._check_schedules(client, inventory),
            *self._check_dependencies(client, inventory),
        )
        for item in potential_issues:
            if isinstance(item, InternalValidatorException):
                yield item
            else:
                findings_by_url[item.url].append(item)

        failure_by_url: dict[str, _Failure] = {}
        for failure in self._find_failures(client, inventory, now):
            if isinstance(failure, InternalValidatorException):
                yield failure
            else:
                failure_by_url[failure.url] = failure

        yield from self._report(findings_by_url, failure_by_url)

    def _load_inventory(self, client: ToolkitClient) -> tuple[_Inventory, list[InternalValidatorException]]:
        inventory = _Inventory()
        exceptions: list[InternalValidatorException] = []
        try:
            inventory.transformations = client.transformations.list(limit=None)
            inventory.transformation_schedules = client.transformations.schedules.list(limit=None)
        except ToolkitAPIError as error:
            exceptions.append(self._retrieve_failed("transformations", error))
        try:
            inventory.functions = client.functions.list(limit=None)
            inventory.function_schedules = client.functions.schedules.list(limit=None)
        except ToolkitAPIError as error:
            exceptions.append(self._retrieve_failed("functions", error))
        try:
            inventory.workflow_versions = client.workflows.versions.list(limit=None)
            inventory.workflow_triggers = client.workflows.triggers.list(limit=None)
        except ToolkitAPIError as error:
            exceptions.append(self._retrieve_failed("workflows", error))
        try:
            inventory.hosted_extractor_jobs = client.hosted_extractors.jobs.list(limit=None)
            inventory.hosted_extractor_sources = client.hosted_extractors.sources.list(limit=None)
            inventory.hosted_extractor_destinations = client.hosted_extractors.destinations.list(limit=None)
        except ToolkitAPIError as error:
            exceptions.append(self._retrieve_failed("hosted extractors", error))
        return inventory, exceptions

    def _retrieve_failed(self, what: str, error: ToolkitAPIError) -> InternalValidatorException:
        return InternalValidatorException(message=f"Failed to retrieve {what}: {error}", source=self.DISPLAY_NAME)

    # 1. Credentials
    def _check_credentials(
        self, client: ToolkitClient, inventory: _Inventory, now: int
    ) -> Iterable[_Finding | InternalValidatorException]:
        for certificate in self._certificates(client, inventory):
            if finding := self._expiry_finding(certificate, certificate.expires_at, now):
                yield finding
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
            elif session.expiration_time is not None:
                status = (
                    f" The session {credential.session_id} is {session.status.casefold()}." if session.status else ""
                )
                if finding := self._expiry_finding(credential, session.expiration_time, now, status):
                    yield finding

    @staticmethod
    def _invalid_finding(credential: _Credential | _Certificate, reason: str) -> _Finding:
        return _Finding(
            url=credential.url,
            severity="error",
            code="INVALID-CREDENTIALS",
            message=f"{credential.name} are invalid, {reason}",
            fix=credential.fix,
            explains="any",
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
        function_by_id = {function.id: function for function in inventory.functions}
        for schedule in inventory.function_schedules:
            if schedule.session_id is None or schedule.function_id is None:
                continue
            credentials.append(
                _Credential(
                    url=_function_url(client, schedule.function_id),
                    name=f"The credentials of the schedule '{schedule.name}' of the function "
                    f"'{_function_name(schedule, function_by_id)}'",
                    fix="Rotate the credentials by recreating the schedule, for example, by deploying it again "
                    "with Toolkit.",
                    session_id=schedule.session_id,
                )
            )
        destination_by_id = {
            destination.external_id: destination for destination in inventory.hosted_extractor_destinations
        }
        for job in inventory.hosted_extractor_jobs:
            destination = destination_by_id.get(job.destination_id)
            if destination is None or destination.session_id is None:
                continue
            credentials.append(
                _Credential(
                    url=_hosted_extractor_job_url(client, job),
                    name=f"The credentials of the hosted extractor destination '{destination.external_id}'",
                    fix="Rotate the credentials of the destination, for example, by deploying it again with Toolkit.",
                    session_id=destination.session_id,
                )
            )
        return credentials

    @staticmethod
    def _certificates(client: ToolkitClient, inventory: _Inventory) -> list[_Certificate]:
        certificates: list[_Certificate] = []
        source_by_id = {source.external_id: source for source in inventory.hosted_extractor_sources}
        for job in inventory.hosted_extractor_jobs:
            source = source_by_id.get(job.source_id)
            if not isinstance(source, KafkaSourceResponse | MQTTSourceResponse | RESTSourceResponse):
                continue
            for kind, certificate in (("CA", source.ca_certificate), ("authentication", source.auth_certificate)):
                if certificate is None or certificate.expires_at is None:
                    continue
                certificates.append(
                    _Certificate(
                        url=_hosted_extractor_job_url(client, job),
                        name=f"The {kind} certificate credentials of the hosted extractor source "
                        f"'{source.external_id}'",
                        fix=f"Upload a new {kind} certificate to the source, for example, by deploying it again "
                        "with Toolkit.",
                        expires_at=certificate.expires_at,
                    )
                )
        return certificates

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
    def _expiry_finding(
        cls, credential: _Credential | _Certificate, expires_at: int, now: int, status: str = ""
    ) -> _Finding | None:
        remaining = expires_at - now
        expiry = ms_to_datetime(expires_at).strftime("%Y-%m-%d %H:%M UTC")
        if remaining <= 0:
            return cls._invalid_finding(
                credential, f"they expired {_humanize_duration(-remaining)} ago, on {expiry}.{status}"
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
            message=f"{credential.name} will expire in {_humanize_duration(remaining)}, on {expiry}.{status}",
            fix=credential.fix,
        )

    # 2. Schedules

    def _check_schedules(self, client: ToolkitClient, inventory: _Inventory) -> Iterable[_Finding]:
        for kind, schedules in self._schedules(client, inventory).items():
            schedule_indices_by_minute: dict[int, list[int]] = defaultdict(list)
            for index, schedule in enumerate(schedules):
                try:
                    minutes = cron_minutes_of_week(schedule.cron_expression, utc_offset_minutes(schedule.timezone))
                except ValueError:
                    continue
                for minute in minutes:
                    schedule_indices_by_minute[minute].append(index)
            minutes_by_herd: dict[tuple[int, ...], list[int]] = defaultdict(list)
            for minute, indices in schedule_indices_by_minute.items():
                if len(indices) >= THUNDERING_HERD_THRESHOLD:
                    minutes_by_herd[tuple(indices)].append(minute)
            # A resource can be in several herds, report only the one it starts with most often, then the largest.
            herd_by_url: dict[str, tuple[int, ...]] = {}
            for indices in sorted(minutes_by_herd, key=lambda herd: (len(minutes_by_herd[herd]), len(herd))):
                for index in indices:
                    herd_by_url[schedules[index].url] = indices
            for url, indices in herd_by_url.items():
                herd = [schedules[index] for index in indices]
                yield _Finding(
                    url=url,
                    severity="warning",
                    code="CONGESTED-SCHEDULE",
                    message=(
                        f"{len(herd)} {kind} start at the same time, "
                        f"{_describe_start_times(minutes_by_herd[indices])}: "
                        f"{_humanize_names([schedule.name for schedule in herd])}. Starting many at once can exceed "
                        "concurrency limits, which delays or fails runs."
                    ),
                    fix=f"Spread the start times of the {kind} over different minutes, for example, "
                    "'7 * * * *' instead of '0 * * * *'.",
                    explains="concurrency",
                )

    @staticmethod
    def _schedules(client: ToolkitClient, inventory: _Inventory) -> dict[str, list[_Schedule]]:
        """The active schedules of each type of compute."""
        schedules: dict[str, list[_Schedule]] = defaultdict(list)
        transformation_by_external_id = {
            transformation.external_id: transformation for transformation in inventory.transformations
        }
        for transformation_schedule in inventory.transformation_schedules:
            transformation = transformation_by_external_id.get(transformation_schedule.external_id)
            if transformation_schedule.is_paused or transformation is None:
                continue
            schedules["transformations"].append(
                _Schedule(
                    url=_transformation_url(client, transformation),
                    name=f"'{transformation.external_id}'",
                    cron_expression=transformation_schedule.interval,
                )
            )
        function_by_id = {function.id: function for function in inventory.functions}
        for function_schedule in inventory.function_schedules:
            if function_schedule.function_id is None:
                continue
            schedules["function schedules"].append(
                _Schedule(
                    url=_function_url(client, function_schedule.function_id),
                    name=f"'{function_schedule.name}' of '{_function_name(function_schedule, function_by_id)}'",
                    cron_expression=function_schedule.cron_expression,
                )
            )
        for trigger in inventory.workflow_triggers:
            rule = trigger.trigger_rule
            if trigger.is_paused or not isinstance(rule, ScheduleTriggerRule):
                continue
            schedules["workflow triggers"].append(
                _Schedule(
                    url=_workflow_version_url(client, trigger.workflow_external_id, trigger.workflow_version),
                    name=f"'{trigger.external_id}'",
                    cron_expression=rule.cron_expression,
                    timezone=rule.timezone,
                )
            )
        return schedules

    # 3. Dependencies

    def _check_dependencies(
        self, client: ToolkitClient, inventory: _Inventory
    ) -> Iterable[_Finding | InternalValidatorException]:
        known: set[tuple[type[ResourceIO], Identifier]] = {
            *((TransformationIO, item.as_id()) for item in inventory.transformations),
            *(
                (FunctionIO, ExternalId(external_id=item.external_id))
                for item in inventory.functions
                if item.external_id
            ),
            *((WorkflowVersionIO, item.as_id()) for item in inventory.workflow_versions),
            *((WorkflowIO, ExternalId(external_id=item.workflow_external_id)) for item in inventory.workflow_versions),
            *(
                (HostedExtractorSourceIO, ExternalId(external_id=item.external_id))
                for item in inventory.hosted_extractor_sources
            ),
            *((HostedExtractorDestinationIO, item.as_id()) for item in inventory.hosted_extractor_destinations),
        }
        references_by_crud: dict[type[ResourceIO], dict[Identifier, list[_Reference]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for reference in self._references(client, inventory):
            if isinstance(reference, InternalValidatorException):
                yield reference
            elif (reference.crud, reference.identifier) not in known:
                references_by_crud[reference.crud][reference.identifier].append(reference)

        for crud_cls, references_by_identifier in references_by_crud.items():
            crud = crud_cls(client)
            try:
                existing = {crud.get_id(item) for item in crud.retrieve(list(references_by_identifier))}
            except ToolkitAPIError as error:
                yield InternalValidatorException(
                    message=f"Failed to verify the existence of {crud.kind} resources in CDF: {error}",
                    source=self.DISPLAY_NAME,
                )
                continue
            for identifier, references in references_by_identifier.items():
                if identifier in existing:
                    continue
                for reference in references:
                    target = f"{reference.label} {quote_identifier(identifier)}"
                    yield _Finding(
                        url=reference.url,
                        severity="error",
                        code="MISSING-REFERENCED-RESOURCE",
                        message=f"{reference.referrer} {reference.verb} the {target}, which does not exist.",
                        fix=f"Create the {target}, or stop using it.",
                        explains="any",
                    )

    def _references(
        self, client: ToolkitClient, inventory: _Inventory
    ) -> Iterable[_Reference | InternalValidatorException]:
        has_sql_parser = self._has_sql_parser()
        transformation_io = TransformationIO(client)
        for transformation in inventory.transformations:
            url = _transformation_url(client, transformation)
            name = f"transformation '{transformation.external_id}'"
            yield from self._dependency_references(transformation_io, transformation, url, name)
            if has_sql_parser:
                yield from self._query_references(transformation, url, name)
        workflow_version_io = WorkflowVersionIO(client)
        for workflow_version in inventory.workflow_versions:
            url = _workflow_version_url(client, workflow_version.workflow_external_id, workflow_version.version)
            name = f"workflow '{workflow_version.workflow_external_id}' version '{workflow_version.version}'"
            yield from self._dependency_references(workflow_version_io, workflow_version, url, name)
        workflow_trigger_io = WorkflowTriggerIO(client)
        for trigger in inventory.workflow_triggers:
            url = client.config.create_api_url(f"/workflows/triggers/{quote(trigger.external_id, safe='')}/history")
            name = f"workflow trigger '{trigger.external_id}'"
            yield from self._dependency_references(workflow_trigger_io, trigger, url, name)
        hosted_extractor_job_io = HostedExtractorJobIO(client)
        for job in inventory.hosted_extractor_jobs:
            url = _hosted_extractor_job_url(client, job)
            name = f"hosted extractor job '{job.external_id}'"
            yield from self._dependency_references(hosted_extractor_job_io, job, url, name)

    def _dependency_references(
        self, crud: ResourceIO, resource: Any, url: str, name: str
    ) -> Iterable[_Reference | InternalValidatorException]:
        """The dependencies of the resource, found by reading it back in the YAML format."""
        try:
            dumped = {**_NOT_RETURNED_BY_CDF.get(type(crud), {}), **crud.dump_resource(resource)}
            loaded = crud.validate_object(dumped, extra="ignore")
        except (ToolkitAPIError, ToolkitError, ValidationError) as error:
            yield InternalValidatorException(
                message=f"Failed to find the dependencies of the {name}: {error}", source=self.DISPLAY_NAME
            )
            return
        for dependency_crud, identifier in crud.get_dependencies(loaded):
            verb, label = _VERB_AND_LABEL_BY_DEPENDENCY.get(dependency_crud, ("uses", dependency_crud.kind))
            yield _Reference(
                url=url, referrer=f"The {name}", verb=verb, label=label, crud=dependency_crud, identifier=identifier
            )

    def _query_references(
        self, transformation: TransformationResponse, url: str, name: str
    ) -> Iterable[_Reference | InternalValidatorException]:
        try:
            sources = get_transformation_sources(transformation.query)
        except Exception as error:
            yield InternalValidatorException(
                message=f"Failed to parse the query of the {name}: {error}", source=self.DISPLAY_NAME
            )
            return
        for source in sources:
            if isinstance(source, RawTableId):
                yield _Reference(
                    url=url,
                    referrer=f"The {name}",
                    verb="reads from",
                    label="RAW table",
                    crud=RawTableIO,
                    identifier=source,
                )

    # 4. Failures

    def _find_failures(
        self, client: ToolkitClient, inventory: _Inventory, now: int
    ) -> Iterable[_Failure | InternalValidatorException]:
        yield from self._transformation_failures(client, inventory.transformations)
        yield from self._function_failures(client, inventory.functions, now)
        yield from self._workflow_failures(client, inventory.workflow_versions, now)
        yield from self._hosted_extractor_failures(client, inventory.hosted_extractor_jobs)

    @staticmethod
    def _transformation_failures(
        client: ToolkitClient, transformations: Sequence[TransformationResponse]
    ) -> Iterable[_Failure]:
        for transformation in transformations:
            last_job = transformation.last_finished_job or {}
            if last_job.get("status") != "Failed":
                continue
            error = last_job.get("error")
            yield _Failure(
                url=_transformation_url(client, transformation),
                message=f"The last run of the transformation '{transformation.external_id}' failed"
                f"{_with_reason(error if isinstance(error, str) else None)}",
                fix="Fix the error in the query or configuration of the transformation, then run it again.",
            )

    def _function_failures(
        self, client: ToolkitClient, functions: Sequence[FunctionResponse], now: int
    ) -> Iterable[_Failure | InternalValidatorException]:
        call_filter = FunctionCallFilter(start_time=EpochTimestampRange(min_=now - RUN_HISTORY_DAYS * _DAY_MS))
        for function in functions:
            url = _function_url(client, function.id)
            name = function.external_id or function.name
            if function.status == "Failed":
                yield _Failure(
                    url=url,
                    message=f"The function '{name}' failed to deploy"
                    f"{_with_reason(function.error.message if function.error else None)}",
                    fix="Fix the error in the code or requirements of the function, then deploy it again.",
                )
                continue
            try:
                calls = client.functions.calls.list(function.id, filter=call_filter, limit=None)
            except ToolkitAPIError as error:
                yield self._retrieve_failed(f"the calls of the function '{name}'", error)
                continue
            finished = [call for call in calls if call.status != "Running"]
            if not finished:
                continue
            last_call = max(finished, key=lambda call: call.start_time)
            if (outcome := _FAILED_FUNCTION_CALLS.get(last_call.status)) is None:
                continue
            logs_url = client.config.create_api_url(f"/functions/{function.id}/calls/{last_call.id}/logs")
            yield _Failure(
                url=url,
                message=f"The last call of the function '{name}' {outcome}{_with_reason(last_call.error)}",
                fix=f"Check the logs of the call at {logs_url}, and fix the code of the function or its input data.",
                concurrency_limited=last_call.status == "ConcurrencyLimitExceeded",
            )

    def _workflow_failures(
        self, client: ToolkitClient, workflow_versions: Sequence[WorkflowVersionResponse], now: int
    ) -> Iterable[_Failure | InternalValidatorException]:
        if not workflow_versions:
            return
        version_ids = {workflow_version.as_id() for workflow_version in workflow_versions}
        try:
            executions = client.workflows.executions.list(
                created_time_start=now - RUN_HISTORY_DAYS * _DAY_MS, limit=None
            )
        except ToolkitAPIError as error:
            yield self._retrieve_failed("workflow executions", error)
            return
        last_by_version_id: dict[WorkflowVersionId, WorkflowExecutionResponse] = {}
        for execution in executions:
            if execution.status == "RUNNING" or execution.version is None:
                continue
            version_id = execution.as_workflow_id()
            if version_id not in version_ids:
                continue
            last = last_by_version_id.get(version_id)
            if last is None or execution.created_time > last.created_time:
                last_by_version_id[version_id] = execution
        for version_id, execution in last_by_version_id.items():
            if (outcome := _FAILED_WORKFLOW_EXECUTIONS.get(execution.status)) is None:
                continue
            yield _Failure(
                url=_workflow_version_url(client, version_id.workflow_external_id, version_id.version),
                message=f"The last execution of the workflow '{version_id.workflow_external_id}' version "
                f"'{version_id.version}' {outcome}{_with_reason(self._workflow_failure_reason(client, execution))}",
                fix="Fix the failed task of the workflow, then run the workflow again.",
            )

    @staticmethod
    def _workflow_failure_reason(client: ToolkitClient, execution: WorkflowExecutionResponse) -> str | None:
        try:
            detailed = client.workflows.executions.retrieve([execution.as_id()], ignore_unknown_ids=True)
        except ToolkitAPIError:
            return execution.reason_for_incompletion
        failed_tasks = [
            f"the task '{task.external_id}' failed with: {task.reason_for_incompletion or task.status}"
            for item in detailed
            for task in item.executed_tasks
            if task.status in _FAILED_TASK_STATUSES
        ]
        return "; ".join(failed_tasks) or execution.reason_for_incompletion

    @staticmethod
    def _hosted_extractor_failures(
        client: ToolkitClient, jobs: Sequence[HostedExtractorJobResponse]
    ) -> Iterable[_Failure]:
        for job in jobs:
            if job.status is None or (fix := _FAILED_HOSTED_EXTRACTOR_JOBS.get(job.status)) is None:
                continue
            try:
                logs = client.hosted_extractors.jobs.list_logs(job=job.external_id, limit=10)
            except ToolkitAPIError:
                logs = []
            reasons = [next((log.message for log in logs if log.type == job.status and log.message), None)]
            if job.status == "transform_error":
                try:
                    messages = client.hosted_extractors.jobs.list_dlq_messages(job=job.external_id, limit=1)
                except ToolkitAPIError:
                    # The dead letter queue is not enabled for the job, or the token cannot read RAW.
                    messages = []
                reasons.extend(
                    f"for example, a message failed with {message.columns.error_category}: {message.columns.error}"
                    for message in messages
                )
            yield _Failure(
                url=_hosted_extractor_job_url(client, job),
                message=f"The hosted extractor job '{job.external_id}' is failing with status '{job.status}'"
                f"{_with_reason('; '.join(reason for reason in reasons if reason) or None)}",
                fix=fix,
            )

    # 5. Report

    @staticmethod
    def _report(findings_by_url: dict[str, list[_Finding]], failure_by_url: dict[str, _Failure]) -> Iterable[Insight]:
        alpha = Flags.ALPHA_RULES.is_enabled()
        for url in dict.fromkeys([*findings_by_url, *failure_by_url]):
            findings = findings_by_url.get(url, [])
            failure = failure_by_url.get(url)
            if failure is not None and not any(finding.explains_failure(failure) for finding in findings):
                yield BuildError(
                    code="FAILED-EXECUTION", message=failure.message, fix=failure.fix, source_url=url, alpha=alpha
                )
            for finding in findings:
                if failure is not None and finding.explains_failure(failure):
                    yield BuildError(
                        code=finding.code,
                        message=f"{failure.message} Likely cause: {finding.message}",
                        fix=finding.fix,
                        source_url=url,
                        alpha=alpha,
                    )
                else:
                    yield _INSIGHT_BY_SEVERITY[finding.severity](
                        code=finding.code, message=finding.message, fix=finding.fix, source_url=url, alpha=alpha
                    )


def _transformation_url(client: ToolkitClient, transformation: TransformationResponse) -> str:
    return client.config.create_api_url(f"/transformations/{transformation.id}")


def _function_url(client: ToolkitClient, function_id: int) -> str:
    return client.config.create_api_url(f"/functions/{function_id}")


def _workflow_version_url(client: ToolkitClient, workflow_external_id: str, version: str) -> str:
    return client.config.create_api_url(
        f"/workflows/{quote(workflow_external_id, safe='')}/versions/{quote(version, safe='')}"
    )


def _hosted_extractor_job_url(client: ToolkitClient, job: HostedExtractorJobResponse) -> str:
    return client.config.create_api_url(f"/hostedextractors/jobs/logs?job={quote(job.external_id, safe='')}")


def _function_name(schedule: FunctionScheduleResponse, function_by_id: dict[int, FunctionResponse]) -> str:
    if schedule.function_external_id:
        return schedule.function_external_id
    function = function_by_id.get(schedule.function_id) if schedule.function_id is not None else None
    if function is None:
        return str(schedule.function_id)
    return function.external_id or function.name


def _with_reason(reason: str | None) -> str:
    if not reason:
        return "."
    if len(reason) > _MAX_ERROR_LENGTH:
        reason = f"{reason[:_MAX_ERROR_LENGTH]}..."
    return f": {reason.rstrip('.')}."


def _humanize_duration(ms: int) -> str:
    if ms >= _DAY_MS:
        days = ms // _DAY_MS
        return f"{days} day{'s' if days > 1 else ''}"
    if ms >= _HOUR_MS:
        hours = ms // _HOUR_MS
        return f"{hours} hour{'s' if hours > 1 else ''}"
    return "less than an hour"


def _humanize_names(names: Sequence[str]) -> str:
    if len(names) <= _MAX_LISTED_NAMES:
        return humanize_collection(names)
    listed = sorted(names)[:_MAX_LISTED_NAMES]
    return f"{', '.join(listed)} and {len(names) - _MAX_LISTED_NAMES} more"


def _describe_start_times(minutes_of_week: Sequence[int]) -> str:
    times = sorted({minute % MINUTES_PER_DAY for minute in minutes_of_week})
    weekdays = sorted({minute // MINUTES_PER_DAY for minute in minutes_of_week})
    clock_times = [f"{time_of_day // 60:02d}:{time_of_day % 60:02d}" for time_of_day in times]
    if len(times) == 24 and len({time_of_day % 60 for time_of_day in times}) == 1:
        minute = times[0] % 60
        description = "every hour, on the hour" if minute == 0 else f"every hour, {minute} minutes past the hour"
    elif len(times) <= 3:
        description = f"at {humanize_collection(clock_times, sort=False)} UTC"
    else:
        description = f"{len(times)} times a day, for example, at {clock_times[0]} UTC"
    if len(weekdays) < 7:
        description += f" on {humanize_collection([_WEEKDAY_NAMES[day] for day in weekdays], sort=False)}"
    return description
