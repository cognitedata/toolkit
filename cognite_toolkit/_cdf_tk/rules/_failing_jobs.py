from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import TypeAlias

from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import WorkflowVersionId
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    EpochTimestampRange,
    FunctionCallFilter,
    WorkflowExecutionFilter,
)
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import HostedExtractorJobResponse
from cognite_toolkit._cdf_tk.client.resource_classes.transformation import TransformationResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import WorkflowExecutionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import WorkflowVersionResponse
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeploymentResult
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildError,
    Insight,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import (
    FunctionIO,
    HostedExtractorJobIO,
    ResourceIO,
    TransformationIO,
    WorkflowIO,
    WorkflowVersionIO,
)
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms
from cognite_toolkit._cdf_tk.yaml_classes import WorkflowVersionYAML

from ._base import (
    InternalValidatorException,
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
)

FailingCDFResource: TypeAlias = (
    WorkflowVersionResponse | FunctionResponse | TransformationResponse | HostedExtractorJobResponse
)
Dependencies: TypeAlias = set[tuple[type[ResourceIO], Identifier]]

FAILED_WORKFLOW_STATUSES = ["FAILED", "TERMINATED", "TIMED_OUT"]
FAILED_FUNCTION_CALL_STATUSES = frozenset({"failed", "timeout", "concurrencylimitexceeded"})
FAILED_HOSTED_EXTRACTOR_JOB_STATUSES = frozenset(
    {"startup_error", "connection_error", "transform_error", "cdf_write_error"}
)


@dataclass
class FailingJob:
    """A job that is failing in CDF.

    The job is kept as retrieved from CDF, together with its dependencies, such that the cause of the
    failure can be diagnosed, and the suggested_fix set, without retrieving it again.
    """

    job_type: str
    job: FailingCDFResource
    reason: str
    url: str
    failed_executions: int | None = None
    dependencies: Dependencies = field(default_factory=set)
    suggested_fix: str | None = None

    @property
    def identifier(self) -> Identifier:
        return self.job.as_id()


class FailingJobRuleSet(ToolkitRuleSet):
    """Checks for failing jobs, i.e. workflows, transformations, functions, and hosted extractors that
    are currently failing in CDF.
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
        failed_executions = client.workflows.executions.list(
            filter=WorkflowExecutionFilter(status=FAILED_WORKFLOW_STATUSES, created_time_start=self.from_time_epoch),
            limit=None,
        )
        failed_executions_by_id: dict[WorkflowVersionId, list[WorkflowExecutionResponse]] = defaultdict(list)
        for execution in failed_executions:
            if execution.version is None:
                continue
            failed_executions_by_id[
                WorkflowVersionId(workflow_external_id=execution.workflow_external_id, version=execution.version)
            ].append(execution)
        if not failed_executions_by_id:
            return []

        io = WorkflowVersionIO(client)
        failures: list[FailingJob] = []
        for workflow in io.retrieve(list(failed_executions_by_id.keys())):
            executions = failed_executions_by_id[workflow.as_id()]
            latest = max(executions, key=lambda e: e.created_time)
            failures.append(
                FailingJob(
                    job_type="Workflow",
                    job=workflow,
                    reason=latest.reason_for_incompletion or "Unknown reason",
                    url=client.config.create_api_url(f"/workflows/{workflow.workflow_external_id}/executions/"),
                    failed_executions=len(executions),
                    dependencies=self._workflow_dependencies(io, workflow),
                )
            )
        return failures

    @staticmethod
    def _workflow_dependencies(io: WorkflowVersionIO, workflow: WorkflowVersionResponse) -> Dependencies:
        try:
            workflow_yaml = WorkflowVersionYAML.model_validate(io.dump_resource(workflow))
        except ValidationError:
            return set()
        return set(io.get_dependencies(workflow_yaml))

    def _load_failing_functions(self, client: ToolkitClient, skip: Dependencies) -> list[FailingJob]:
        failures: list[FailingJob] = []
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
            failed_calls = [call for call in calls if call.status.casefold() in FAILED_FUNCTION_CALL_STATUSES]
            if not failed_calls:
                continue
            latest = max(failed_calls, key=lambda c: c.start_time)
            failures.append(
                FailingJob(
                    job_type="Function",
                    job=function,
                    reason=latest.error or "Unknown reason",
                    url=client.config.create_api_url(f"/functions/{function.id}/"),
                    failed_executions=len(failed_calls),
                )
            )
        return failures

    def _load_failing_transformations(self, client: ToolkitClient, skip: Dependencies) -> list[FailingJob]:
        failures: list[FailingJob] = []
        for transformation in client.transformations.list(limit=None):
            if (TransformationIO, transformation.as_id()) in skip:
                continue
            url = client.config.create_api_url(f"/transformations/{transformation.id}/")
            last_job = transformation.last_finished_job
            if last_job and last_job.status.casefold() == "failed" and last_job.created_time >= self.from_time_epoch:
                failures.append(
                    FailingJob(
                        job_type="Transformation",
                        job=transformation,
                        reason=last_job.error or "Unknown reason",
                        url=url,
                        failed_executions=self._count_failed_jobs(client, transformation.id),
                    )
                )
            elif transformation.blocked:
                # Blocked transformations are always reported, even if they were blocked before the lookback
                # period, as they are still failing.
                failures.append(
                    FailingJob(
                        job_type="Transformation",
                        job=transformation,
                        reason=f"Blocked: {transformation.blocked.reason}",
                        url=url,
                    )
                )
        return failures

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

    @staticmethod
    def _load_failing_hosted_extractor_jobs(client: ToolkitClient) -> list[FailingJob]:
        return [
            FailingJob(
                job_type="Hosted Extractor",
                job=job,
                reason=job.status,
                url=client.config.create_api_url(f"/hosted-extractors/{job.external_id}/"),
            )
            for job in client.hosted_extractors.jobs.list(limit=None)
            if job.status and job.status.casefold() in FAILED_HOSTED_EXTRACTOR_JOB_STATUSES
        ]

    def _report_failures(self, failures: Sequence[FailingJob]) -> Iterable[Insight | InternalValidatorException]:
        """Report the failures as insights"""
        for failure in failures:
            message = f"Job {failure.job_type} with identifier {failure.identifier} is failing. Error: {failure.reason}"
            if failure.failed_executions is not None:
                message += f"\nNumber of failed executions in the last {self.lookback_period.days} days: {failure.failed_executions}."
            yield BuildError(
                code="FAILING-JOB",
                message=message,
                source_url=failure.url,
                fix=failure.suggested_fix,
                alpha=Flags.ALPHA_RULES.is_enabled(),
            )
