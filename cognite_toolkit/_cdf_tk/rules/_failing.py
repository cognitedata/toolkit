from collections import defaultdict
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.identifiers import WorkflowVersionId
from cognite_toolkit._cdf_tk.client.request_classes.filters import EpochTimestampRange, FunctionCallFilter
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_execution import WorkflowExecutionResponse
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


@dataclass
class FailingResource:
    resource_type: str
    identifier: Identifier
    reason: str
    url: str
    failed_executions: int | None = None
    suggested_fix: str | None = None


class Failing(ToolkitRuleSet):
    """Checks for failing resources, i.e. workflows, transformations, functions, and hosted extractors that
    are currently failing in CDF.
    """

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
        lookback_period: timedelta = timedelta(days=7),
        resources: Sequence[ResourceRef] | None = None,
        results: Sequence[DeploymentResult] | None = None,
        client: ToolkitClient | None = None,
    ) -> None:
        super().__init__(resources=resources, results=results, client=client)
        self.lookback_period = lookback_period
        self.from_time_epoc = datetime_to_ms(datetime.now(UTC) - self.lookback_period)

    def get_status(self) -> RuleSetStatus:
        raise NotImplementedError()

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        failures = self._load_failures(self.client)
        yield from self._report_failures(failures)

    def _load_failures(self, client: ToolkitClient) -> Sequence[FailingResource]:
        """Loads the inventory of resources from CDF and yields insights for any failing resources."""
        failures: list[FailingResource] = []
        failed_executions = client.workflows.executions.list(
            statuses=["FAILED", "TERMINATED", "TIMED_OUT"], created_time_start=self.from_time_epoc, limit=None
        )
        failed_executions_by_id: dict[WorkflowVersionId, list[WorkflowExecutionResponse]] = defaultdict(list)
        for execution in failed_executions:
            failed_executions_by_id[
                WorkflowVersionId(workflow_external_id=execution.workflow_external_id, version=execution.version)
            ].append(execution)

        io = WorkflowVersionIO(client)

        failing_workflow_version = io.retrieve(list(failed_executions_by_id.keys()))
        already_reported_: set[tuple[type[ResourceIO], Identifier]] = set()
        for workflow in failing_workflow_version:
            dumped = io.dump_resource(workflow)
            workflow_yaml = WorkflowVersionYAML.model_validate(dumped)
            already_reported_.update(io.get_dependencies(workflow_yaml))
            executions = failed_executions_by_id[workflow.as_id()]

            latest = max(executions, key=lambda e: e.created_time)
            failures.append(
                FailingResource(
                    resource_type="Workflow",
                    identifier=workflow.as_id(),
                    reason=latest.reason_for_incompletion or "Unknown reason",
                    url=client.config.create_api_url(f"/workflows/{workflow.workflow_external_id}/executions/"),
                    failed_executions=len(executions),
                )
            )

        for function_list in client.functions.iterate(limit=None):
            for function in function_list:
                if (FunctionIO, function.as_id()) in already_reported_:
                    continue
                if function.last_called is None:
                    continue
                function_calls = client.functions.calls.list(
                    function_id=function.id,
                    filter=FunctionCallFilter(
                        start_time=EpochTimestampRange(min=self.from_time_epoc),
                    ),
                    limit=None,
                )
                failing_function_calls = [
                    call
                    for call in function_calls
                    if call.status.casefold() in {"failed", "timeout", "concurrencylimitexceeded"}
                ]
                if failing_function_calls:
                    latest_call = max(failing_function_calls, key=lambda c: c.start_time)
                    failures.append(
                        FailingResource(
                            resource_type="Function",
                            identifier=function.as_id(),
                            reason=latest_call.error or "Unknown reason",
                            url=client.config.create_api_url(f"/functions/{function.id}/"),
                            failed_executions=len(failing_function_calls),
                        )
                    )

        # # For transformations, always include blocked executions, even if their failure is not within the lookback period, as they are still failing.
        # for transformation_list in client.transformations.iterate(limit=None):
        #     for transformation in transformation_list:
        #         if transformation.last_finished_job and transformation.last_finished_job...
        #             ...

        for job_list in client.hosted_extractors.jobs.iterate(limit=None):
            for job in job_list:
                if (HostedExtractorJobIO, job.as_id()) in already_reported_:
                    continue
                if job.status and job.status.casefold() in {
                    "startup_error",
                    "connection_error",
                    "transform_error",
                    "cdf_with_error",
                }:
                    failures.append(
                        FailingResource(
                            resource_type="Hosted Extractor",
                            identifier=job.as_id(),
                            reason=job.status,
                            url=client.config.create_api_url(f"/hosted-extractors/{job.external_id}/"),
                        )
                    )

        return failures

    def _report_failures(self, failures: Sequence[FailingResource]) -> Iterable[Insight | InternalValidatorException]:
        """Report the failures as insights"""
        for failure in failures:
            message = f"Resource {failure.resource_type} with identifier {failure.identifier} is failing. Error: {failure.reason}"
            if failure.failed_executions is not None:
                message += f"\nNumber of failed executions in the last {self.lookback_period.days} days: {failure.failed_executions}."
            yield BuildError(
                code="FAILING-RESOURCE",
                message=message,
                source_url=failure.url,
                fix=failure.suggested_fix,
                alpha=Flags.ALPHA_RULES.is_enabled(),
            )
