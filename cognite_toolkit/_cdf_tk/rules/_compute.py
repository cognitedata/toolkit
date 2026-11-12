import importlib.util
import time
from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Literal
from urllib.parse import quote

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import (
    DataModelId,
    ExternalId,
    InternalId,
    RawTableId,
    SpaceId,
    ViewId,
    WorkflowVersionId,
)
from cognite_toolkit._cdf_tk.client.request_classes.filters import EpochTimestampRange, FunctionCallFilter
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import FunctionScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    CustomFormat,
    HostedExtractorJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    HostedExtractorSourceResponseUnion,
    KafkaSourceResponse,
    MQTTSourceResponse,
    RESTSourceResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    DataModelSource,
    RawDataSource,
    SequenceRowDataSource,
    TransformationResponse,
    ViewDataSource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation_schedule import TransformationScheduleResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import WorkflowExecutionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    ScheduleTriggerRule,
    WorkflowTriggerResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    FunctionAppTaskParameters,
    FunctionTaskParameters,
    SimulationTaskParameters,
    SubworkflowRef,
    SubworkflowTaskParameters,
    Task,
    TransformationTaskParameters,
    WorkflowVersionResponse,
)
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
    InsightCode,
    InternalValidatorException,
    Recommendation,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    DataModelIO,
    FunctionIO,
    FunctionScheduleIO,
    HostedExtractorDestinationIO,
    HostedExtractorJobIO,
    HostedExtractorMappingIO,
    HostedExtractorSourceIO,
    RawTableIO,
    ResourceIO,
    SequenceIO,
    SimulatorRoutineIO,
    SpaceIO,
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
    """Credentials a compute resource runs with. They expire when either the session or the certificate does."""

    url: str
    name: str
    fix: str
    session_id: int | None = None
    expires_at: int | None = None


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

    First, finds potential issues: credentials that expire soon, many resources scheduled to start at the same
    time, and resources they depend on that do not exist. Then, reports the resources whose last run failed, with
    the potential issue as the cause if one explains the failure. The remaining potential issues are reported as is.
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
        credentials = self._credentials(client, inventory)
        session_ids = sorted({credential.session_id for credential in credentials if credential.session_id})
        expiry_by_session_id, unverified = self._session_expiry(client, session_ids)
        if unverified:
            yield unverified
        for credential in credentials:
            if credential.session_id is not None:
                expires_at = expiry_by_session_id.get(credential.session_id)
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
        source_by_id = {source.external_id: source for source in inventory.hosted_extractor_sources}
        for job in inventory.hosted_extractor_jobs:
            url = _hosted_extractor_job_url(client, job)
            destination = destination_by_id.get(job.destination_id)
            if destination is not None and destination.session_id is not None:
                credentials.append(
                    _Credential(
                        url=url,
                        name=f"The credentials of the hosted extractor destination '{destination.external_id}'",
                        fix="Rotate the credentials of the destination, for example, by deploying it again "
                        "with Toolkit.",
                        session_id=destination.session_id,
                    )
                )
            source = source_by_id.get(job.source_id)
            if not isinstance(source, KafkaSourceResponse | MQTTSourceResponse | RESTSourceResponse):
                continue
            for kind, certificate in (("CA", source.ca_certificate), ("authentication", source.auth_certificate)):
                if certificate is None:
                    continue
                credentials.append(
                    _Credential(
                        url=url,
                        name=f"The {kind} certificate of the hosted extractor source '{source.external_id}'",
                        fix=f"Upload a new {kind} certificate to the source, for example, by deploying it again "
                        "with Toolkit.",
                        expires_at=certificate.expires_at,
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
                explains="any",
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
            for indices, minutes in minutes_by_herd.items():
                herd = [schedules[index] for index in indices]
                message = (
                    f"{len(herd)} {kind} start at the same time, {_describe_start_times(minutes)}: "
                    f"{_humanize_names([schedule.name for schedule in herd])}. Starting many at once can exceed "
                    "concurrency limits, which delays or fails runs."
                )
                for url in dict.fromkeys(schedule.url for schedule in herd):
                    yield _Finding(
                        url=url,
                        severity="warning",
                        code="CONGESTED-SCHEDULE",
                        message=message,
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
        yield from self._transformation_references(client, inventory.transformations)
        for workflow_version in inventory.workflow_versions:
            url = _workflow_version_url(client, workflow_version.workflow_external_id, workflow_version.version)
            for task in _tasks(workflow_version.workflow_definition.tasks):
                referrer = (
                    f"The task '{task.external_id}' of the workflow '{workflow_version.workflow_external_id}' "
                    f"version '{workflow_version.version}'"
                )
                if reference := _task_reference(task, url, referrer):
                    yield reference
        for trigger in inventory.workflow_triggers:
            yield _Reference(
                url=client.config.create_api_url(f"/workflows/triggers/{quote(trigger.external_id, safe='')}/history"),
                referrer=f"The workflow trigger '{trigger.external_id}'",
                verb="runs",
                label="workflow version",
                crud=WorkflowVersionIO,
                identifier=WorkflowVersionId(
                    workflow_external_id=trigger.workflow_external_id, version=trigger.workflow_version
                ),
            )
        for job in inventory.hosted_extractor_jobs:
            url = _hosted_extractor_job_url(client, job)
            referrer = f"The hosted extractor job '{job.external_id}'"
            yield _Reference(
                url=url,
                referrer=referrer,
                verb="reads from",
                label="hosted extractor source",
                crud=HostedExtractorSourceIO,
                identifier=ExternalId(external_id=job.source_id),
            )
            yield _Reference(
                url=url,
                referrer=referrer,
                verb="writes to",
                label="hosted extractor destination",
                crud=HostedExtractorDestinationIO,
                identifier=ExternalId(external_id=job.destination_id),
            )
            if isinstance(job.format, CustomFormat):
                yield _Reference(
                    url=url,
                    referrer=referrer,
                    verb="uses",
                    label="hosted extractor mapping",
                    crud=HostedExtractorMappingIO,
                    identifier=ExternalId(external_id=job.format.mapping_id),
                )

    def _transformation_references(
        self, client: ToolkitClient, transformations: Sequence[TransformationResponse]
    ) -> Iterable[_Reference | InternalValidatorException]:
        has_sql_parser = self._has_sql_parser()
        for transformation in transformations:
            url = _transformation_url(client, transformation)
            referrer = f"The transformation '{transformation.external_id}'"
            destination = transformation.destination
            targets: list[tuple[str, type[ResourceIO], Identifier]] = []
            if isinstance(destination, RawDataSource):
                targets.append(
                    ("RAW table", RawTableIO, RawTableId(db_name=destination.database, name=destination.table))
                )
            elif isinstance(destination, DataModelSource):
                data_model = destination.data_model
                targets.append(
                    (
                        "data model",
                        DataModelIO,
                        DataModelId(
                            space=data_model.space, external_id=data_model.external_id, version=data_model.version
                        ),
                    )
                )
            elif isinstance(destination, ViewDataSource):
                view = destination.view
                targets.append(
                    ("view", ViewIO, ViewId(space=view.space, external_id=view.external_id, version=view.version))
                )
            elif isinstance(destination, SequenceRowDataSource):
                targets.append(("sequence", SequenceIO, ExternalId(external_id=destination.external_id)))
            if isinstance(destination, DataModelSource | ViewDataSource) and destination.instance_space:
                targets.append(("instance space", SpaceIO, SpaceId(space=destination.instance_space)))
            for label, crud, identifier in targets:
                yield _Reference(
                    url=url, referrer=referrer, verb="writes to", label=label, crud=crud, identifier=identifier
                )
            if not has_sql_parser:
                continue
            try:
                sources = get_transformation_sources(transformation.query)
            except Exception as error:
                yield InternalValidatorException(
                    message=f"Failed to parse the query of the transformation '{transformation.external_id}': {error}",
                    source=self.DISPLAY_NAME,
                )
                continue
            for source in sources:
                if isinstance(source, RawTableId):
                    yield _Reference(
                        url=url,
                        referrer=referrer,
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


def _tasks(tasks: Sequence[Task]) -> Iterable[Task]:
    """The tasks, including the tasks of inline subworkflows."""
    for task in tasks:
        yield task
        parameters = task.parameters
        if isinstance(parameters, SubworkflowTaskParameters) and isinstance(parameters.subworkflow, SubworkflowRef):
            yield from _tasks(parameters.subworkflow.tasks)


def _task_reference(task: Task, url: str, referrer: str) -> _Reference | None:
    """The resource the task runs. External IDs starting with '${' are resolved when the workflow runs."""
    parameters = task.parameters
    target: tuple[str, str, type[ResourceIO], Identifier] | None = None
    if isinstance(parameters, FunctionTaskParameters):
        target = ("calls", "function", FunctionIO, ExternalId(external_id=parameters.function.external_id))
    elif isinstance(parameters, FunctionAppTaskParameters):
        target = ("calls", "function", FunctionIO, ExternalId(external_id=parameters.function_app.external_id))
    elif isinstance(parameters, TransformationTaskParameters):
        target = (
            "runs",
            "transformation",
            TransformationIO,
            ExternalId(external_id=parameters.transformation.external_id),
        )
    elif isinstance(parameters, SimulationTaskParameters):
        target = (
            "runs",
            "simulator routine",
            SimulatorRoutineIO,
            ExternalId(external_id=parameters.simulation.routine_external_id),
        )
    elif isinstance(parameters, SubworkflowTaskParameters) and isinstance(parameters.subworkflow, WorkflowVersionId):
        target = ("runs", "workflow version", WorkflowVersionIO, parameters.subworkflow)
    if target is None:
        return None
    verb, label, crud, identifier = target
    if any(value.startswith("${") for value in identifier.dump().values() if isinstance(value, str)):
        return None
    return _Reference(url=url, referrer=referrer, verb=verb, label=label, crud=crud, identifier=identifier)


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
