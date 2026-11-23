from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, ParamSpec, TypeVar

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
from cognite_toolkit._cdf_tk.data_classes._insights import Insight, InternalValidatorException, Recommendation
from cognite_toolkit._cdf_tk.resource_ios import (
    DataModelIO,
    ExtractionPipelineIO,
    FunctionIO,
    HostedExtractorDestinationIO,
    HostedExtractorJobIO,
    HostedExtractorMappingIO,
    HostedExtractorSourceIO,
    RawDatabaseIO,
    RawTableIO,
    ResourceType,
    SpaceIO,
    TransformationIO,
    ViewIO,
    WorkflowIO,
)
from cognite_toolkit._cdf_tk.rules import ValidationInput
from cognite_toolkit._cdf_tk.rules._base import RuleSetStatus, ToolkitRuleSet
from cognite_toolkit._cdf_tk.utils.time import datetime_to_ms

MAX_PARALLEL_REQUESTS = 8

T = TypeVar("T")
R = TypeVar("R")
P = ParamSpec("P")

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
    url: str


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
        # All requests to CDF run in this pool, which limits the number of parallel requests.
        self._request_executor: ThreadPoolExecutor | None = None

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
            (SpaceIO.kind, lambda: self._unused_spaces(client, created_before_epoch)),
            (WorkflowIO.kind, lambda: self._unused_workflows(client, created_before_epoch)),
            (TransformationIO.kind, lambda: self._unused_transformations(client, created_before_epoch)),
            (FunctionIO.kind, lambda: self._unused_functions(client, created_before_epoch)),
            (HostedExtractorJobIO.kind, lambda: self._unused_hosted_extractors(client, created_before_epoch)),
            (ViewIO.kind, lambda: self._unused_views(client, created_before_epoch)),
            (DataModelIO.kind, lambda: self._unused_data_models(client, created_before_epoch)),
            (ExtractionPipelineIO.kind, lambda: self._unused_extraction_pipelines(client, created_before_epoch)),
            (RawDatabaseIO.kind, lambda: self._unused_raw_databases(client, created_before_epoch)),
            (RawTableIO.kind, lambda: self._unused_tables(client, created_before_epoch)),
        ]
        # The checks only orchestrate; the requests themselves run in the request executor. Using a separate
        # pool for the checks avoids deadlocks when a check waits for its own requests to complete.
        with (
            ThreadPoolExecutor(max_workers=MAX_PARALLEL_REQUESTS) as request_executor,
            ThreadPoolExecutor(max_workers=len(checks)) as check_executor,
        ):
            self._request_executor = request_executor
            try:
                futures = [(kind, check_executor.submit(check)) for kind, check in checks]
                for kind, future in futures:
                    try:
                        result = future.result()
                    except ToolkitAPIError as e:
                        yield InternalValidatorException(
                            message=f"Failed to check for unused {kind.lower()} in CDF: {e}", source=kind
                        )
                        continue
                    for unused in result if isinstance(result, list) else [result]:
                        if unused.identifiers:
                            yield unused
            finally:
                self._request_executor = None

    @property
    def _executor(self) -> ThreadPoolExecutor:
        if self._request_executor is None:
            raise RuntimeError("The request executor is only available while loading unused resources.")
        return self._request_executor

    def _submit(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Schedules a single request in the request executor."""
        return self._executor.submit(func, *args, **kwargs)

    def _request(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        """Runs a single request in the request executor and waits for the result."""
        return self._submit(func, *args, **kwargs).result()

    def _request_each(self, func: Callable[[T], R], items: Iterable[T]) -> list[R]:
        """Runs func for each item in parallel in the request executor."""
        return list(self._executor.map(func, items))

    def _report_unused(self, unused_resources: list[_UnusedResourceTypes]) -> Iterable[Insight]:
        for unused in unused_resources:
            identifiers_str = "\n - ".join(str(identifier) for identifier in unused.identifiers)
            yield Recommendation(
                code="EMPTY-RESOURCE",
                message=f"{len(unused.identifiers)} unused {unused.type.kind.lower()}(s) found in CDF: {unused.message}",
                source_url=unused.url,
                fix=f"Delete the unused:\n - {identifiers_str}",
            )

    @property
    def _never_run_message(self) -> str:
        return f"Created more than {self.created_before.days} days ago and has never run"

    @property
    def _created_before_message(self) -> str:
        return f"Created more than {self.created_before.days} days ago"

    def _unused_spaces(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        results = self._request(client.statistics.spaces.list)
        unused_space_ids = [space.space for space in results if space.is_empty]
        if not unused_space_ids:
            return _UnusedResourceTypes(
                type=SpaceIO.as_resource_type(),
                identifiers=[],
                message=f"{self._created_before_message} and is empty",
                url="/models/spaces",
            )
        unused_spaces = self._request(client.spaces.retrieve, [SpaceId(space=space) for space in unused_space_ids])
        return _UnusedResourceTypes(
            type=SpaceIO.as_resource_type(),
            identifiers=[space.as_id() for space in unused_spaces if space.created_time < created_before_epoch],
            message=f"{self._created_before_message} and is empty",
            url="/models/spaces",
        )

    def _unused_workflows(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        candidates = [
            workflow
            for workflow in self._request(client.workflows.list, limit=None)
            if workflow.created_time < created_before_epoch
        ]
        has_run = self._request_each(
            lambda workflow: bool(
                client.workflows.executions.list(
                    filter=WorkflowExecutionFilter(
                        workflow_filters=[WorkflowFilterItem(external_id=workflow.external_id)]
                    ),
                    limit=1,
                )
            ),
            candidates,
        )
        unused = [workflow.as_id() for workflow, run in zip(candidates, has_run) if not run]
        return _UnusedResourceTypes(
            type=WorkflowIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._never_run_message}. There are no executions of this workflow",
            url="/workflows",
        )

    def _unused_transformations(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        transformations = self._request(
            client.transformations.list,
            filter=TransformationFilter(created_time=EpochTimestampRange(max=created_before_epoch)),
            limit=None,
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
            url="/transformations",
        )

    def _unused_functions(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        unused: Sequence[Identifier] = [
            ExternalId(external_id=function.external_id) if function.external_id else InternalId(id=function.id)
            for function in self._request(client.functions.list, limit=None)
            if function.created_time < created_before_epoch and function.last_called is None
        ]
        return _UnusedResourceTypes(
            type=FunctionIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._never_run_message}. This function has never been called",
            url="/functions",
        )

    def _unused_hosted_extractors(self, client: ToolkitClient, created_before_epoch: int) -> list[_UnusedResourceTypes]:
        """Hosted extractor jobs that have never run, together with the sources, destinations, and mappings
        not used by any job."""
        hosted = client.hosted_extractors
        jobs_future = self._submit(hosted.jobs.list, limit=None)
        sources_future = self._submit(hosted.sources.list, limit=None)
        destinations_future = self._submit(hosted.destinations.list, limit=None)
        mappings_future = self._submit(hosted.mappings.list, limit=None)
        jobs = jobs_future.result()
        candidate_jobs = [job for job in jobs if job.created_time < created_before_epoch]
        has_logs = self._request_each(
            lambda job: bool(hosted.jobs.list_logs(job=job.external_id, limit=1)), candidate_jobs
        )
        unused_jobs = [job.as_id() for job, logs in zip(candidate_jobs, has_logs) if not logs]
        used_sources = {job.source_id for job in jobs}
        used_destinations = {job.destination_id for job in jobs}
        used_mappings = {job.format.mapping_id for job in jobs if isinstance(job.format, CustomFormat)}
        not_used_by_job = f"{self._created_before_message} and is not used by any hosted extractor job"
        return [
            _UnusedResourceTypes(
                type=HostedExtractorJobIO.as_resource_type(),
                identifiers=unused_jobs,
                message=f"{self._never_run_message}. There are no logs of this hosted extractor job",
                url="/hostedextractors/jobs",
            ),
            _UnusedResourceTypes(
                type=HostedExtractorSourceIO.as_resource_type(),
                identifiers=[
                    ExternalId(external_id=source.external_id)
                    for source in sources_future.result()
                    if source.created_time < created_before_epoch and source.external_id not in used_sources
                ],
                message=not_used_by_job,
                url="/hostedextractors/sources",
            ),
            _UnusedResourceTypes(
                type=HostedExtractorDestinationIO.as_resource_type(),
                identifiers=[
                    destination.as_id()
                    for destination in destinations_future.result()
                    if destination.created_time < created_before_epoch
                    and destination.external_id not in used_destinations
                ],
                message=not_used_by_job,
                url="/hostedextractors/destinations",
            ),
            _UnusedResourceTypes(
                type=HostedExtractorMappingIO.as_resource_type(),
                identifiers=[
                    mapping.as_id()
                    for mapping in mappings_future.result()
                    if mapping.created_time < created_before_epoch and mapping.external_id not in used_mappings
                ],
                message=not_used_by_job,
                url="/hostedextractors/mappings",
            ),
        ]

    def _unused_views(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        """Views without instances. Views used for records are skipped, as records are not instances."""
        views = self._request(client.views.list, filter=ViewFilter(include_global=False), limit=None)
        candidates = [
            (view.as_id(), instance_type)
            for view in views
            if view.created_time < created_before_epoch and view.used_for in INSTANCE_TYPES_BY_USED_FOR
            for instance_type in INSTANCE_TYPES_BY_USED_FOR[view.used_for]
        ]
        has_instances = self._request_each(lambda candidate: self._has_instances(client, *candidate), candidates)
        used_views = {view_id for (view_id, _), has in zip(candidates, has_instances) if has}
        unused = list(dict.fromkeys(view_id for view_id, _ in candidates if view_id not in used_views))
        return _UnusedResourceTypes(
            type=ViewIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._created_before_message} and has no instances",
            url="/models/views",
        )

    @staticmethod
    def _has_instances(client: ToolkitClient, view: ViewId, instance_type: Literal["node", "edge"]) -> bool:
        response = client.instances.aggregate(
            view, instance_type=instance_type, aggregates=[CountAggregate(property="externalId")]
        )
        return any(aggregate.value for item in response.items for aggregate in item.aggregates)

    def _unused_data_models(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        data_models = self._request(client.data_models.list, filter=DataModelFilter(include_global=False), limit=None)
        unused = [
            data_model.as_id()
            for data_model in data_models
            if data_model.created_time < created_before_epoch and not data_model.views
        ]
        return _UnusedResourceTypes(
            type=DataModelIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._created_before_message} and has no views",
            url="/models/datamodels",
        )

    def _unused_extraction_pipelines(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        unused = [
            pipeline.as_id()
            for pipeline in self._request(client.extraction_pipelines.list, limit=None)
            if pipeline.created_time < created_before_epoch and pipeline.last_seen is None
        ]
        return _UnusedResourceTypes(
            type=ExtractionPipelineIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._never_run_message}. No extractor has reported a run to this extraction pipeline",
            url="/extpipes",
        )

    def _unused_raw_databases(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        candidates = [
            database
            for database in self._request(client.raw.databases.list, limit=None)
            if database.created_time < created_before_epoch
        ]
        has_tables = self._request_each(
            lambda database: bool(client.raw.tables.list(database.name, limit=1)), candidates
        )
        unused = [database.as_id() for database, has in zip(candidates, has_tables) if not has]
        return _UnusedResourceTypes(
            type=RawDatabaseIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._created_before_message} and has no tables",
            url="/raw/dbs",
        )

    def _unused_tables(self, client: ToolkitClient, created_before_epoch: int) -> _UnusedResourceTypes:
        databases = self._request(client.raw.databases.list, limit=None)
        tables_by_database = self._request_each(
            lambda database: client.raw.tables.list(database.name, limit=None), databases
        )
        candidates = [
            (database.name, table)
            for database, tables in zip(databases, tables_by_database)
            for table in tables
            if table.created_time < created_before_epoch
        ]
        has_rows = self._request_each(
            lambda candidate: bool(client.raw.tables.rows.list(candidate[0], candidate[1].name, limit=1)), candidates
        )
        unused = [table.as_id() for (_, table), has in zip(candidates, has_rows) if not has]
        return _UnusedResourceTypes(
            type=RawTableIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._created_before_message} and has no rows",
            url="/raw/tables",
        )
