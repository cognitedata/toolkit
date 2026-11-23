from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, InternalId, SpaceId, ViewId
from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    DataModelFilter,
    EpochTimestampRange,
    TransformationFilter,
    ViewFilter,
    WorkflowExecutionFilter,
    WorkflowFilterItem,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import CountAggregate
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import CustomFormat
from cognite_toolkit._cdf_tk.data_classes._insights import Insight, InternalValidatorException
from cognite_toolkit._cdf_tk.resource_ios import (
    DataModelIO,
    ExtractionPipelineIO,
    FunctionIO,
    HostedExtractorDestinationIO,
    HostedExtractorJobIO,
    HostedExtractorMappingIO,
    HostedExtractorSourceIO,
    RawDatabaseIO,
    ResourceType,
    SpaceIO,
    TransformationIO,
    ViewIO,
    WorkflowIO,
)
from cognite_toolkit._cdf_tk.rules import ValidationInput
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitRuleSet
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms

INSTANCE_TYPES_BY_USED_FOR: dict[str, list[Literal["node", "edge"]]] = {
    "node": ["node"],
    "edge": ["edge"],
    "all": ["node", "edge"],
}


@dataclass
class _UnusedResourceTypes:
    type: ResourceType
    identifiers: Sequence[Identifier]
    message: str


class UnusedRuleSet(ToolkitRuleSet):
    """Checks for resources in CDF that are not used.

    Jobs, i.e., workflows, transformations, functions, and hosted extractor jobs, are unused if they were created
    before created_before and have never run. Other resources are unused if they were created before created_before
    and are empty, for example, views without instances, or are not referenced by any other resource.
    """

    DISPLAY_NAME = "Unused resources"
    RESOURCE_TYPES = frozenset(
        {
            WorkflowIO.as_resource_type(),
            FunctionIO.as_resource_type(),
            HostedExtractorJobIO.as_resource_type(),
            HostedExtractorSourceIO.as_resource_type(),
            HostedExtractorDestinationIO.as_resource_type(),
            HostedExtractorMappingIO.as_resource_type(),
            TransformationIO.as_resource_type(),
            SpaceIO.as_resource_type(),
            ViewIO.as_resource_type(),
            DataModelIO.as_resource_type(),
            ExtractionPipelineIO.as_resource_type(),
            RawDatabaseIO.as_resource_type(),
        }
    )
    CONTEXT = frozenset({"cdf"})

    def __init__(self, client: ToolkitClient | None = None, created_before: timedelta = timedelta(days=7)) -> None:
        super().__init__(client=client)
        self.created_before = created_before

    def get_status(self, data: ValidationInput) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(code="skip", message="A client is required to check for unused resources in CDF.")
        else:
            return RuleSetStatus(code="ready", message="Ready to check for unused resources in CDF.")

    def validate(self, data: ValidationInput) -> Iterable[Insight | InternalValidatorException]:
        if self.client is None:
            return
        created_before_epoch = datetime_to_ms(datetime.now(UTC) - self.created_before)
        unused_resources: list[_UnusedResourceTypes] = []
        for loaded in self._load_unused(self.client, created_before_epoch):
            if isinstance(loaded, InternalValidatorException):
                yield loaded
            else:
                unused_resources.append(loaded)
        yield from self._report_unused(unused_resources)

    def _load_unused(
        self, client: ToolkitClient, created_before_epoch: int
    ) -> Iterable[_UnusedResourceTypes | InternalValidatorException]:
        checks: list[tuple[str, Callable[[], _UnusedResourceTypes | list[_UnusedResourceTypes]]]] = [
            (SpaceIO.kind, lambda: self._unused_spaces(client)),
            (WorkflowIO.kind, lambda: self._unused_workflows(client, created_before_epoch)),
            (TransformationIO.kind, lambda: self._unused_transformations(client, created_before_epoch)),
            (FunctionIO.kind, lambda: self._unused_functions(client, created_before_epoch)),
            (HostedExtractorJobIO.kind, lambda: self._unused_hosted_extractors(client, created_before_epoch)),
            (ViewIO.kind, lambda: self._unused_views(client, created_before_epoch)),
            (DataModelIO.kind, lambda: self._unused_data_models(client, created_before_epoch)),
            (ExtractionPipelineIO.kind, lambda: self._unused_extraction_pipelines(client, created_before_epoch)),
            (RawDatabaseIO.kind, lambda: self._unused_raw_databases(client, created_before_epoch)),
        ]
        for kind, check in checks:
            try:
                result = check()
            except ToolkitAPIError as e:
                yield InternalValidatorException(
                    message=f"Failed to check for unused {kind.lower()} in CDF: {e}", source=kind
                )
                continue
            for unused in result if isinstance(result, list) else [result]:
                if unused.identifiers:
                    yield unused

    def _report_unused(self, unused_resources: list[_UnusedResourceTypes]) -> Iterable[Insight]:
        raise NotImplementedError()

    @property
    def _never_run_message(self) -> str:
        return f"Created more than {self.created_before.days} days ago and has never run"

    @property
    def _created_before_message(self) -> str:
        return f"Created more than {self.created_before.days} days ago"

    @classmethod
    def _unused_spaces(cls, client: ToolkitClient) -> _UnusedResourceTypes:
        results = client.statistics.spaces.list()
        unused_spaces = [space.space for space in results if space.is_empty]
        return _UnusedResourceTypes(
            type=SpaceIO.as_resource_type(),
            identifiers=[SpaceId(space=space) for space in unused_spaces],
            message="There are no instances, views, containers, or other resources in this space",
        )

    def _unused_workflows(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        candidates = [
            workflow for workflow in client.workflows.list(limit=None) if workflow.created_time < created_before_epoch
        ]
        unused = [
            workflow.as_id()
            for workflow in candidates
            if not client.workflows.executions.list(
                filter=WorkflowExecutionFilter(workflow_filters=[WorkflowFilterItem(external_id=workflow.external_id)]),
                limit=1,
            )
        ]
        return _UnusedResourceTypes(
            type=WorkflowIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._never_run_message}. There are no executions of this workflow",
        )

    def _unused_transformations(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        transformations = client.transformations.list(
            filter=TransformationFilter(created_time=EpochTimestampRange(max=created_before_epoch)), limit=None
        )
        unused = [
            transformation.as_id()
            for transformation in transformations
            if transformation.last_finished_job is None and transformation.running_job is None
        ]
        return _UnusedResourceTypes(
            type=TransformationIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._never_run_message}. There are no jobs of this transformation",
        )

    def _unused_functions(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        unused: Sequence[Identifier] = [
            ExternalId(external_id=function.external_id) if function.external_id else InternalId(id=function.id)
            for function in client.functions.list(limit=None)
            if function.created_time < created_before_epoch and function.last_called is None
        ]
        return _UnusedResourceTypes(
            type=FunctionIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._never_run_message}. This function has never been called",
        )

    def _unused_hosted_extractors(self, client: ToolkitClient, created_before_epoch: int) -> list[_UnusedResourceTypes]:
        """Hosted extractor jobs that have never run, together with the sources, destinations, and mappings
        not used by any job."""
        jobs = client.hosted_extractors.jobs.list(limit=None)
        unused_jobs = [
            job.as_id()
            for job in jobs
            if job.created_time < created_before_epoch
            and not client.hosted_extractors.jobs.list_logs(job=job.external_id, limit=1)
        ]
        used_sources = {job.source_id for job in jobs}
        used_destinations = {job.destination_id for job in jobs}
        used_mappings = {job.format.mapping_id for job in jobs if isinstance(job.format, CustomFormat)}
        not_used_by_job = f"{self._created_before_message} and is not used by any hosted extractor job"
        return [
            _UnusedResourceTypes(
                type=HostedExtractorJobIO.as_resource_type(),
                identifiers=unused_jobs,
                message=f"{self._never_run_message}. There are no logs of this hosted extractor job",
            ),
            _UnusedResourceTypes(
                type=HostedExtractorSourceIO.as_resource_type(),
                identifiers=[
                    ExternalId(external_id=source.external_id)
                    for source in client.hosted_extractors.sources.list(limit=None)
                    if source.created_time < created_before_epoch and source.external_id not in used_sources
                ],
                message=not_used_by_job,
            ),
            _UnusedResourceTypes(
                type=HostedExtractorDestinationIO.as_resource_type(),
                identifiers=[
                    destination.as_id()
                    for destination in client.hosted_extractors.destinations.list(limit=None)
                    if destination.created_time < created_before_epoch
                    and destination.external_id not in used_destinations
                ],
                message=not_used_by_job,
            ),
            _UnusedResourceTypes(
                type=HostedExtractorMappingIO.as_resource_type(),
                identifiers=[
                    mapping.as_id()
                    for mapping in client.hosted_extractors.mappings.list(limit=None)
                    if mapping.created_time < created_before_epoch and mapping.external_id not in used_mappings
                ],
                message=not_used_by_job,
            ),
        ]

    def _unused_views(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        """Views without instances. Views used for records are skipped, as records are not instances."""
        views = client.views.list(filter=ViewFilter(include_global=False), limit=None)
        unused = [
            view.as_id()
            for view in views
            if view.created_time < created_before_epoch
            and view.used_for in INSTANCE_TYPES_BY_USED_FOR
            and not self._has_instances(client, view.as_id(), INSTANCE_TYPES_BY_USED_FOR[view.used_for])
        ]
        return _UnusedResourceTypes(
            type=ViewIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._created_before_message} and has no instances",
        )

    @staticmethod
    def _has_instances(client: ToolkitClient, view: ViewId, instance_types: list[Literal["node", "edge"]]) -> bool:
        for instance_type in instance_types:
            response = client.instances.aggregate(
                view, instance_type=instance_type, aggregates=[CountAggregate(property="externalId")]
            )
            if any(aggregate.value for item in response.items for aggregate in item.aggregates):
                return True
        return False

    def _unused_data_models(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        data_models = client.data_models.list(filter=DataModelFilter(include_global=False), limit=None)
        unused = [
            data_model.as_id()
            for data_model in data_models
            if data_model.created_time < created_before_epoch and not data_model.views
        ]
        return _UnusedResourceTypes(
            type=DataModelIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._created_before_message} and has no views",
        )

    def _unused_extraction_pipelines(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        unused = [
            pipeline.as_id()
            for pipeline in client.extraction_pipelines.list(limit=None)
            if pipeline.created_time < created_before_epoch and pipeline.last_seen is None
        ]
        return _UnusedResourceTypes(
            type=ExtractionPipelineIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._never_run_message}. No extractor has reported a run to this extraction pipeline",
        )

    def _unused_raw_databases(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        unused = [
            database.as_id()
            for database in client.raw.databases.list(limit=None)
            if database.created_time < created_before_epoch and not client.raw.tables.list(database.name, limit=1)
        ]
        return _UnusedResourceTypes(
            type=RawDatabaseIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._created_before_message} and has no tables",
        )
