from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TypeAlias

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId, WorkflowExecutionId, WorkflowVersionId
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    EpochTimestampRange,
    FunctionCallFilter,
    WorkflowExecutionFilter,
)
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_call import FunctionCallResponse
from cognite_toolkit._cdf_tk.client.resource_classes.group import AllScope, SessionsAcl
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    CustomFormat,
    HostedExtractorJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import (
    AssetCentricDataSource,
    DataModelSource,
    Destination,
    RawDataSource,
    SequenceRowDataSource,
    TransformationResponse,
    UnknownDestinationDefinition,
    ViewDataSource,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import (
    WorkflowExecutionDetailedResponse,
    WorkflowExecutionResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    CDFRequest,
    CDFTaskParameters,
    SimulationTaskParameters,
    SubworkflowRef,
    SubworkflowTaskParameters,
    Task,
    TransformationTaskParameters,
    WorkflowVersionResponse,
)
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeploymentResult
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildError,
    Insight,
)
from cognite_toolkit._cdf_tk.exceptions import AuthorizationError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    FunctionIO,
    HostedExtractorJobIO,
    ResourceIO,
    TransformationIO,
    WorkflowIO,
    WorkflowVersionIO,
)
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms
from cognite_toolkit._cdf_tk.yaml_classes import (
    HostedExtractorJobYAML,
    ToolkitResource,
    TransformationYAML,
    WorkflowVersionYAML,
)

from ._base import (
    InternalValidatorException,
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
    quote_identifier,
)

FailingCDFResource: TypeAlias = (
    WorkflowVersionResponse | FunctionResponse | TransformationResponse | HostedExtractorJobResponse
)
Dependencies: TypeAlias = set[tuple[type[ResourceIO], Identifier]]

FAILED_WORKFLOW_STATUSES = frozenset({"FAILED", "TERMINATED", "TIMED_OUT"})
FINISHED_WORKFLOW_STATUSES = ["COMPLETED", *sorted(FAILED_WORKFLOW_STATUSES)]
FAILED_FUNCTION_CALL_STATUSES = frozenset({"failed", "timeout", "concurrencylimitexceeded"})
RUNNING_FUNCTION_CALL_STATUS = "running"
FAILED_HOSTED_EXTRACTOR_JOB_STATUSES = frozenset(
    {"startup_error", "connection_error", "transform_error", "cdf_write_error"}
)
VALID_SESSION_STATUSES = frozenset({"READY", "ACTIVE"})
COMPLETED_TASK_STATUS = "COMPLETED"
READ_ONLY_CDF_REQUEST_SUFFIXES = ("/list", "/byids", "/search", "/aggregate", "/query", "/sync", "/filter")


@dataclass
class FailingJob:
    """A job that is failing in CDF.

    The job is kept as retrieved from CDF, together with its dependencies and the sessions it runs with,
    such that the cause of the failure can be diagnosed, and the suggested_fix set, without retrieving it again.
    """

    job_type: str
    job: FailingCDFResource
    url: str
    reason: str | None = None
    failed_executions: int | None = None
    impact: set[str] = field(default_factory=set)
    dependencies: Dependencies = field(default_factory=set)
    # Transformations have separate source and destination sessions, and a function can be
    # called by multiple schedules.
    session_ids: set[int] = field(default_factory=set)
    # The latest execution of a failing workflow, used to find the tasks that did not complete.
    execution_id: WorkflowExecutionId | None = None
    suggested_fix: str | None = None

    @property
    def identifier(self) -> Identifier:
        return self.job.as_id()


class FailingJobRuleSet(ToolkitRuleSet):
    """Checks for failing jobs, i.e. workflows, transformations, functions, and hosted extractors that
    are currently failing in CDF.

    A job is currently failing if its latest finished execution failed. Jobs that failed earlier in the
    lookback period, but have succeeded since, are not reported.
    """

    DISPLAY_NAME = "Failing Jobs"
    RESOURCE_TYPES = frozenset(
        {
            WorkflowIO.as_resource_type(),
            FunctionIO.as_resource_type(),
            HostedExtractorJobIO.as_resource_type(),
            TransformationIO.as_resource_type(),
        }
    )
    CONTEXT = frozenset({"cdf"})

    def __init__(
        self,
        resources: Sequence[ResourceRef] | None = None,
        results: Sequence[DeploymentResult] | None = None,
        client: ToolkitClient | None = None,
        lookback_period: timedelta = timedelta(days=7),
    ) -> None:
        super().__init__(resources=resources, results=results, client=client)
        self.lookback_period = lookback_period
        self.from_time_epoch = datetime_to_ms(datetime.now(UTC) - self.lookback_period)

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(
                code="skip",
                message="Cannot check for failing jobs without a ToolkitClient. Provide one to the rule set.",
            )
        return RuleSetStatus(
            code="ready",
            message=f"Checks for failing jobs in CDF within the last {self.lookback_period.days} days.",
        )

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        failures = self._load_failures(self.client)
        yield from self._set_workflow_impact(self.client, failures)
        yield from self._diagnose_failures(self.client, failures)
        yield from self._report_failures(failures)

    def _load_failures(self, client: ToolkitClient) -> list[FailingJob]:
        """Loads the failing jobs from CDF.

        Functions and transformations used by a failing workflow are not reported separately, as their failure
        is reported through the workflow.
        """
        failures = self._load_failing_workflows(client)
        used_by_failing_workflows: Dependencies = set().union(*(failure.dependencies for failure in failures))
        failures.extend(self._load_failing_functions(client, used_by_failing_workflows))
        failures.extend(self._load_failing_transformations(client, used_by_failing_workflows))
        failures.extend(self._load_failing_hosted_extractor_jobs(client))
        return failures

    def _load_failing_workflows(self, client: ToolkitClient) -> list[FailingJob]:
        finished_executions = client.workflows.executions.list(
            filter=WorkflowExecutionFilter(status=FINISHED_WORKFLOW_STATUSES, created_time_start=self.from_time_epoch),
            limit=None,
        )
        executions_by_id: dict[WorkflowVersionId, list[WorkflowExecutionResponse]] = defaultdict(list)
        for execution in finished_executions:
            if execution.version is None:
                continue
            executions_by_id[
                WorkflowVersionId(workflow_external_id=execution.workflow_external_id, version=execution.version)
            ].append(execution)
        failing_by_id = {
            workflow_id: executions
            for workflow_id, executions in executions_by_id.items()
            if max(executions, key=lambda e: e.created_time).status.upper() in FAILED_WORKFLOW_STATUSES
        }
        if not failing_by_id:
            return []

        io = WorkflowVersionIO(client)
        failures: list[FailingJob] = []
        for workflow in io.retrieve(list(failing_by_id.keys())):
            executions = failing_by_id[workflow.as_id()]
            latest = max(executions, key=lambda e: e.created_time)
            failed_executions = [e for e in executions if e.status.upper() in FAILED_WORKFLOW_STATUSES]
            failures.append(
                FailingJob(
                    job_type="Workflow",
                    job=workflow,
                    reason=latest.reason_for_incompletion,
                    url=f"/workflows/{workflow.workflow_external_id}/executions/",
                    failed_executions=len(failed_executions),
                    dependencies=self._dependencies(io, workflow, WorkflowVersionYAML),
                    execution_id=latest.as_id(),
                )
            )
        return failures

    @staticmethod
    def _dependencies(io: ResourceIO, resource: FailingCDFResource, yaml_cls: type[ToolkitResource]) -> Dependencies:
        try:
            resource_yaml = yaml_cls.model_validate(io.dump_resource(resource))
        except ValidationError:
            return set()
        return set(io.get_dependencies(resource_yaml))

    def _load_failing_functions(self, client: ToolkitClient, skip: Dependencies) -> list[FailingJob]:
        failed_calls_by_function: list[tuple[FunctionResponse, list[FunctionCallResponse]]] = []
        for function in client.functions.list(limit=None):
            if function.last_called is None or function.last_called < self.from_time_epoch:
                continue
            if (FunctionIO, function.as_id()) in skip:
                continue
            calls = client.functions.calls.list(
                function_id=function.id,
                filter=FunctionCallFilter(start_time=EpochTimestampRange(min=self.from_time_epoch)),
                limit=None,
            )
            finished_calls = [call for call in calls if call.status.casefold() != RUNNING_FUNCTION_CALL_STATUS]
            if not finished_calls:
                continue
            if max(finished_calls, key=lambda c: c.start_time).status.casefold() not in FAILED_FUNCTION_CALL_STATUSES:
                continue
            failed_calls = [call for call in finished_calls if call.status.casefold() in FAILED_FUNCTION_CALL_STATUSES]
            failed_calls_by_function.append((function, failed_calls))

        schedule_ids = {
            call.schedule_id
            for _, failed_calls in failed_calls_by_function
            for call in failed_calls
            if call.schedule_id is not None
        }
        session_by_schedule_id = self._session_by_schedule_id(client, schedule_ids)
        failures: list[FailingJob] = []
        for function, failed_calls in failed_calls_by_function:
            latest = max(failed_calls, key=lambda c: c.start_time)
            failures.append(
                FailingJob(
                    job_type="Function",
                    job=function,
                    reason=latest.error,
                    url=f"/functions/{function.id}/",
                    failed_executions=len(failed_calls),
                    session_ids={
                        session_by_schedule_id[call.schedule_id]
                        for call in failed_calls
                        if call.schedule_id is not None and call.schedule_id in session_by_schedule_id
                    },
                )
            )
        return failures

    @staticmethod
    def _session_by_schedule_id(client: ToolkitClient, schedule_ids: set[int]) -> dict[int, int]:
        if not schedule_ids:
            return {}
        schedules = client.functions.schedules.retrieve(
            InternalId.from_ids(sorted(schedule_ids)), ignore_unknown_ids=True
        )
        return {schedule.id: schedule.session_id for schedule in schedules if schedule.session_id is not None}

    def _load_failing_transformations(self, client: ToolkitClient, skip: Dependencies) -> list[FailingJob]:
        io = TransformationIO(client)
        failures: list[FailingJob] = []
        for transformation in client.transformations.list(limit=None):
            if (TransformationIO, transformation.as_id()) in skip:
                continue
            last_job = transformation.last_finished_job
            if last_job and last_job.status.casefold() == "failed" and last_job.created_time >= self.from_time_epoch:
                reason = last_job.error
                failed_executions: int | None = self._count_failed_jobs(client, transformation.id)
            elif transformation.blocked:
                # Blocked transformations are always reported, even if they were blocked before the lookback
                # period, as they are still failing.
                reason = f"Blocked: {transformation.blocked.reason}"
                failed_executions = None
            else:
                continue
            impact = self._get_transformation_impact(transformation.destination)
            failures.append(
                FailingJob(
                    job_type="Transformation",
                    job=transformation,
                    reason=reason,
                    url=f"/transformations/{transformation.id}/",
                    failed_executions=failed_executions,
                    impact=impact,
                    dependencies=self._dependencies(io, transformation, TransformationYAML),
                    session_ids={
                        session.session_id
                        for session in (transformation.source_session, transformation.destination_session)
                        if session is not None
                    },
                )
            )
        return failures

    @staticmethod
    def _get_transformation_impact(destination: Destination) -> set[str]:
        """Returns a description of the resources that are not updated while the transformation is failing."""
        impact: set[str] = set()
        match destination:
            case AssetCentricDataSource():
                impact.add(f"{destination.type.replace('_', ' ')} (asset-centric)")
            case DataModelSource():
                data_model = destination.data_model
                impact.add(f"data model {data_model.space}:{data_model.external_id}(version={data_model.version})")
                if data_model.destination_relationship_from_type:
                    impact.add(
                        f"edges of property {data_model.destination_relationship_from_type}."
                        f"{data_model.destination_type} in space {data_model.space}"
                    )
                else:
                    impact.add(f"instances of type {data_model.destination_type} in space {data_model.space}")
                if destination.instance_space:
                    impact.add(f"instance space {destination.instance_space}")
            case ViewDataSource():
                view = destination.view
                impact.add(f"{destination.type} of view {view.space}:{view.external_id}(version={view.version})")
                if edge_type := destination.edge_type:
                    impact.add(f"edges of type {edge_type.space}:{edge_type.external_id}")
                if destination.instance_space:
                    impact.add(f"instance space {destination.instance_space}")
            case RawDataSource():
                impact.add(f"RAW table {destination.database}.{destination.table}")
            case SequenceRowDataSource():
                impact.add(f"rows of sequence {destination.external_id}")
            case UnknownDestinationDefinition():
                impact.add(f"destination of type {destination.type}")
        return impact

    def _count_failed_jobs(self, client: ToolkitClient, transformation_id: int) -> int:
        """Counts the failed jobs within the lookback period. Jobs are returned newest first, so we stop
        paging once we reach a job older than the lookback period."""
        count = 0
        for jobs in client.transformations.jobs.iterate(transformation_id=transformation_id, limit=None):
            recent_jobs = [job for job in jobs if job.created_time >= self.from_time_epoch]
            count += sum(1 for job in recent_jobs if job.status.casefold() == "failed")
            if len(recent_jobs) < len(jobs):
                break
        return count

    def _load_failing_hosted_extractor_jobs(self, client: ToolkitClient) -> list[FailingJob]:
        failed_jobs = [
            job
            for job in client.hosted_extractors.jobs.list(limit=None)
            if job.status and job.status.casefold() in FAILED_HOSTED_EXTRACTOR_JOB_STATUSES
        ]
        if not failed_jobs:
            return []
        # The credentials of a hosted extractor job are stored on its destination.
        destinations = client.hosted_extractors.destinations.retrieve(
            [
                ExternalId(external_id=external_id)
                for external_id in sorted({job.destination_id for job in failed_jobs})
            ],
            ignore_unknown_ids=True,
        )
        destination_by_id = {destination.external_id: destination for destination in destinations}
        io = HostedExtractorJobIO(client)
        failures: list[FailingJob] = []
        for job in failed_jobs:
            destination = destination_by_id.get(job.destination_id)
            failures.append(
                FailingJob(
                    job_type="Hosted Extractor",
                    job=job,
                    reason=job.status,
                    url=f"/hosted-extractors/{job.external_id}/",
                    impact=self._get_hosted_extractor_impact(job, destination),
                    dependencies=self._dependencies(io, job, HostedExtractorJobYAML),
                    session_ids=(
                        {destination.session_id}
                        if destination is not None and destination.session_id is not None
                        else set()
                    ),
                )
            )
        return failures

    @staticmethod
    def _get_hosted_extractor_impact(
        job: HostedExtractorJobResponse, destination: HostedExtractorDestinationResponse | None
    ) -> set[str]:
        """Returns a description of the resources that are not updated while the hosted extractor is failing."""
        impact: set[str] = set()
        if destination is not None and destination.target_data_set_id is not None:
            impact.add(f"resources in data set with id {destination.target_data_set_id}")
        if isinstance(job.format, CustomFormat):
            impact.add(f"resources written by mapping {job.format.mapping_id}")
        elif data_models := getattr(job.format, "data_models", None):
            impact.add(f"instances in space {data_models.space}")
        return impact

    def _set_workflow_impact(
        self, client: ToolkitClient, failures: Sequence[FailingJob]
    ) -> Iterable[InternalValidatorException]:
        """Sets the impact of the failing workflows from the tasks that did not complete in their latest execution.

        Functions are not included, as we cannot know what they write to.
        """
        failure_by_execution_id = {
            failure.execution_id: failure for failure in failures if failure.execution_id is not None
        }
        if not failure_by_execution_id:
            return
        try:
            executions = client.workflows.executions.retrieve(
                list(failure_by_execution_id.keys()), ignore_unknown_ids=True
            )
        except ToolkitAPIError as e:
            yield InternalValidatorException(
                message=f"Failed to retrieve workflow executions from CDF: {e}", source="Workflow"
            )
            return
        incomplete_by_execution_id = {execution.as_id(): self._incomplete_tasks(execution) for execution in executions}
        transformation_ids = {
            task.parameters.transformation.external_id
            for tasks in incomplete_by_execution_id.values()
            for task in tasks
            if isinstance(task.parameters, TransformationTaskParameters)
        }
        transformation_by_id: dict[str, TransformationResponse] = {}
        if transformation_ids:
            try:
                transformations = client.transformations.retrieve(
                    [ExternalId(external_id=external_id) for external_id in sorted(transformation_ids)],
                    ignore_unknown_ids=True,
                )
            except ToolkitAPIError as e:
                yield InternalValidatorException(
                    message=f"Failed to retrieve transformations from CDF: {e}", source="Transformation"
                )
            else:
                transformation_by_id = {
                    transformation.external_id: transformation for transformation in transformations
                }
        for execution_id, tasks in incomplete_by_execution_id.items():
            if failure := failure_by_execution_id.get(execution_id):
                for task in tasks:
                    failure.impact.update(self._get_task_impact(task, transformation_by_id))

    @classmethod
    def _incomplete_tasks(cls, execution: WorkflowExecutionDetailedResponse) -> list[Task]:
        """Returns the tasks, including those of inline subworkflows, that did not complete in the execution.
        Tasks that were never started, for example because a task they depend on failed, are included."""
        completed = {
            task.external_id for task in execution.executed_tasks if task.status.upper() == COMPLETED_TASK_STATUS
        }
        return [
            task
            for task in cls._flatten_tasks(execution.workflow_definition.tasks)
            if task.external_id not in completed
        ]

    @classmethod
    def _flatten_tasks(cls, tasks: Sequence[Task]) -> Iterable[Task]:
        for task in tasks:
            yield task
            if isinstance(task.parameters, SubworkflowTaskParameters) and isinstance(
                task.parameters.subworkflow, SubworkflowRef
            ):
                yield from cls._flatten_tasks(task.parameters.subworkflow.tasks)

    @classmethod
    def _get_task_impact(cls, task: Task, transformation_by_id: dict[str, TransformationResponse]) -> set[str]:
        """Returns a description of the resources that are not updated by the task. Function tasks
        are ignored, as we cannot know what they write to."""
        match task.parameters:
            case TransformationTaskParameters():
                transformation = transformation_by_id.get(task.parameters.transformation.external_id)
                if transformation is None:
                    return set()
                return cls._get_transformation_impact(transformation.destination)
            case CDFTaskParameters():
                return cls._get_cdf_request_impact(task.parameters.cdf_request)
            case SimulationTaskParameters():
                return {f"results of simulator routine {task.parameters.simulation.routine_external_id}"}
        return set()

    @staticmethod
    def _get_cdf_request_impact(request: CDFRequest) -> set[str]:
        method = request.method.upper()
        path = request.resource_path.rstrip("/")
        if method == "GET" or path.casefold().endswith(READ_ONLY_CDF_REQUEST_SUFFIXES):
            return set()
        return {f"target of CDF request {method} {request.resource_path}"}

    def _diagnose_failures(
        self, client: ToolkitClient, failures: Sequence[FailingJob]
    ) -> Iterable[InternalValidatorException]:
        """Checks whether the dependencies of the failing jobs exist and whether their sessions are valid,
        and sets the suggested fix (and the reason, if unknown) of the jobs where they are not.

        The dependencies and sessions of all failures are checked together to minimize the number of requests.
        """
        dependencies_by_io: dict[type[ResourceIO], set[Identifier]] = defaultdict(set)
        for failure in failures:
            for io_cls, identifier in failure.dependencies:
                dependencies_by_io[io_cls].add(identifier)

        missing_dependencies: Dependencies = set()
        for io_cls, identifiers in dependencies_by_io.items():
            io = io_cls(client)
            try:
                existing = {io.get_id(item) for item in io.retrieve(list(identifiers))}
            except ToolkitAPIError as e:
                yield InternalValidatorException(
                    message=f"Failed to verify existence of {io_cls.kind.lower()} in CDF: {e}",
                    source=io_cls.kind,
                )
                continue
            missing_dependencies.update((io_cls, identifier) for identifier in identifiers - existing)

        invalid_status_by_session_id: dict[int, str] = {}
        can_verify_sessions = True
        if session_ids := set().union(*(failure.session_ids for failure in failures)):
            try:
                # Without sessionsAcl:LIST, retrieving sessions silently returns nothing, which would
                # wrongly mark every session as not found.
                if client.token.verify_acls([SessionsAcl(actions=["LIST"], scope=AllScope())]):
                    can_verify_sessions = False
                else:
                    invalid_status_by_session_id = self._invalid_sessions(client, session_ids)
            except (ToolkitAPIError, AuthorizationError) as e:
                yield InternalValidatorException(message=f"Failed to verify sessions in CDF: {e}", source="Session")

        for failure in failures:
            self._set_suggested_fix(
                failure,
                missing=failure.dependencies & missing_dependencies,
                invalid_sessions={
                    session_id: status
                    for session_id, status in invalid_status_by_session_id.items()
                    if session_id in failure.session_ids
                },
                can_verify_sessions=can_verify_sessions,
            )

    @staticmethod
    def _invalid_sessions(client: ToolkitClient, session_ids: set[int]) -> dict[int, str]:
        """Returns the status of the sessions that are not valid, including those that do not exist."""
        sessions = {
            session.id: session
            for session in client.sessions.retrieve(InternalId.from_ids(sorted(session_ids)), ignore_unknown_ids=True)
        }
        invalid: dict[int, str] = {}
        for session_id in session_ids:
            session = sessions.get(session_id)
            if session is None:
                invalid[session_id] = "not found"
            elif session.status is not None and session.status not in VALID_SESSION_STATUSES:
                invalid[session_id] = session.status.lower().replace("_", " ")
        return invalid

    @staticmethod
    def _set_suggested_fix(
        failure: FailingJob,
        missing: Dependencies,
        invalid_sessions: dict[int, str],
        can_verify_sessions: bool = True,
    ) -> None:
        explanations: list[str] = []
        fixes: list[str] = []
        if missing:
            missing_str = humanize_collection(
                [f"{io_cls.kind} {quote_identifier(identifier)}" for io_cls, identifier in missing]
            )
            explanations.append("It uses resources that do not exist in CDF.")
            fixes.append(f"Create {missing_str} in CDF, or remove the references to them.")
        if invalid_sessions:
            sessions_str = humanize_collection(
                [f"session {session_id} is {status}" for session_id, status in invalid_sessions.items()]
            )
            explanations.append("Its credentials are no longer valid.")
            fixes.append(f"Update the credentials used by the {failure.job_type.lower()}, as {sessions_str}.")
        if failure.session_ids and not can_verify_sessions:
            fixes.append(
                f"Couldn't verify the credentials used by the {failure.job_type.lower()}. "
                "Add sessionsAcl:LIST to your credentials to check them."
            )
        if not fixes:
            return
        failure.suggested_fix = " ".join(fixes)
        if failure.reason is None and explanations:
            failure.reason = " ".join(explanations)

    def _report_failures(self, failures: Sequence[FailingJob]) -> Iterable[Insight | InternalValidatorException]:
        """Report the failures as insights"""
        for failure in failures:
            message = (
                f"Job {failure.job_type} with identifier {failure.identifier} is failing. "
                f"Error: {failure.reason or 'Unknown reason'}"
            )
            if failure.failed_executions is not None:
                message += (
                    f"\nNumber of failed executions in the last {self.lookback_period.days} days: "
                    f"{failure.failed_executions}."
                )
            if failure.impact:
                message += f"\nNot updated while failing: {humanize_collection(failure.impact)}."
            yield BuildError(
                code="FAILING-JOB",
                message=message,
                source_url=failure.url,
                fix=failure.suggested_fix,
                alpha=Flags.ALPHA_RULES.is_enabled(),
            )
