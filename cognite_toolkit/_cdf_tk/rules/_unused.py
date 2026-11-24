from collections.abc import Callable, Iterable, Sequence
from concurrent.futures import Future, ThreadPoolExecutor
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Literal, ParamSpec, TypeAlias, TypeVar

from pydantic import ValidationError

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
    ResourceIO,
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
CHECK_ERRORS = (ToolkitAPIError, ValidationError)

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


CheckResult: TypeAlias = _UnusedResourceTypes | list[_UnusedResourceTypes | InternalValidatorException]


class _Requests:
    """Runs the requests to CDF in a shared pool, which limits the number of parallel requests."""

    def __init__(self, executor: ThreadPoolExecutor) -> None:
        self._executor = executor

    def submit(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> Future[T]:
        """Schedules a single request."""
        return self._executor.submit(func, *args, **kwargs)

    def request(self, func: Callable[P, T], *args: P.args, **kwargs: P.kwargs) -> T:
        """Runs a single request and waits for the result."""
        return self.submit(func, *args, **kwargs).result()

    def each(self, func: Callable[[T], R], items: Iterable[T]) -> list[R]:
        """Runs func for each item in parallel."""
        return list(self._executor.map(func, items))


Check: TypeAlias = Callable[[ToolkitClient, _Requests, int], CheckResult]


class UnusedRuleSet(ToolkitRuleSet):
    """Checks for resources in CDF that are not used.

    Jobs, i.e., workflows, transformations, functions, and hosted extractor jobs, are unused if they were created
    before created_before and have never run. Other resources are unused if they were created before created_before
    and are empty, for example, views without instances, or are not referenced by any other resource.

    Only the latest version of views and data models is checked. Instances are counted by the containers a view
    maps, so older versions mapping the same containers have the same instances as the latest version.
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
            RawTableIO.as_resource_type(),
        }
    )
    CONTEXT = frozenset({"cdf"})

    def __init__(self, client: ToolkitClient | None = None, created_before: timedelta = timedelta(days=14)) -> None:
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
        yield from self._report_unused(self.client, unused_resources)

    def _load_unused(
        self, client: ToolkitClient, created_before_epoch: int
    ) -> Iterable[_UnusedResourceTypes | InternalValidatorException]:
        checks: list[tuple[type[ResourceIO], Check]] = [
            (SpaceIO, self._unused_spaces),
            (WorkflowIO, self._unused_workflows),
            (TransformationIO, self._unused_transformations),
            (FunctionIO, self._unused_functions),
            (HostedExtractorJobIO, self._unused_hosted_extractors),
            (ViewIO, self._unused_views),
            (DataModelIO, self._unused_data_models),
            (ExtractionPipelineIO, self._unused_extraction_pipelines),
            (RawDatabaseIO, self._unused_raw),
        ]
        # The checks only orchestrate; the requests themselves run in the request executor. Using a separate
        # pool for the checks avoids deadlocks when a check waits for its own requests to complete.
        with (
            ThreadPoolExecutor(max_workers=MAX_PARALLEL_REQUESTS) as request_executor,
            ThreadPoolExecutor(max_workers=len(checks)) as check_executor,
        ):
            requests = _Requests(request_executor)
            futures = [
                (io_cls, check_executor.submit(check, client, requests, created_before_epoch))
                for io_cls, check in checks
            ]
            for io_cls, future in futures:
                try:
                    result = future.result()
                except CHECK_ERRORS as e:
                    yield self._failed_check(client, io_cls, e)
                    continue
                for unused in result if isinstance(result, list) else [result]:
                    if isinstance(unused, InternalValidatorException) or unused.identifiers:
                        yield unused

    @staticmethod
    def _failed_check(client: ToolkitClient, io_cls: type[ResourceIO], error: Exception) -> InternalValidatorException:
        return InternalValidatorException(
            message=f"Failed to check for unused {io_cls(client).display_name} in CDF: {error}", source=io_cls.kind
        )

    def _report_unused(self, client: ToolkitClient, unused_resources: list[_UnusedResourceTypes]) -> Iterable[Insight]:
        for unused in unused_resources:
            display_name = unused.type.crud_cls(client).display_name
            identifiers_str = "\n - ".join(str(identifier) for identifier in unused.identifiers)
            yield Recommendation(
                code="UNUSED-RESOURCE",
                message=f"{len(unused.identifiers)} unused {display_name} found in CDF: {unused.message}",
                source_url=unused.url,
                fix=f"Delete the unused:\n - {identifiers_str}",
            )

    @property
    def _never_run_message(self) -> str:
        return f"Created more than {self.created_before.days} days ago and has never run"

    @property
    def _created_before_message(self) -> str:
        return f"Created more than {self.created_before.days} days ago"

    def _unused_spaces(self, client: ToolkitClient, requests: _Requests, created_before_epoch: int) -> CheckResult:
        results = requests.request(client.statistics.spaces.list)
        unused_space_ids = [space.space for space in results if space.is_empty]
        if not unused_space_ids:
            return _UnusedResourceTypes(
                type=SpaceIO.as_resource_type(),
                identifiers=[],
                message=f"{self._created_before_message} and is empty",
                url="/models/spaces",
            )
        unused_spaces = requests.request(client.spaces.retrieve, [SpaceId(space=space) for space in unused_space_ids])
        return _UnusedResourceTypes(
            type=SpaceIO.as_resource_type(),
            identifiers=[
                space.as_id()
                for space in unused_spaces
                if not space.is_global and space.created_time < created_before_epoch
            ],
            message=f"{self._created_before_message} and is empty",
            url="/models/spaces",
        )

    def _unused_workflows(self, client: ToolkitClient, requests: _Requests, created_before_epoch: int) -> CheckResult:
        candidates = [
            workflow
            for workflow in requests.request(client.workflows.list, limit=None)
            if workflow.created_time < created_before_epoch
        ]
        # We do one request here per workflow, if we did a single request for up to 100 workflows
        # (limit for workflow filters) we would have to paginate through all executions to ensure that a
        # workflow has never run, which could be more expensive than doing one request per workflow.
        has_run = requests.each(
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
            message=f"{self._created_before_message} and no executions of this workflow were found",
            url="/workflows",
        )

    def _unused_transformations(
        self, client: ToolkitClient, requests: _Requests, created_before_epoch: int
    ) -> CheckResult:
        transformations = requests.request(
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

    def _unused_functions(self, client: ToolkitClient, requests: _Requests, created_before_epoch: int) -> CheckResult:
        unused: Sequence[Identifier] = [
            ExternalId(external_id=function.external_id) if function.external_id else InternalId(id=function.id)
            for function in requests.request(client.functions.list, limit=None)
            if function.created_time < created_before_epoch and function.last_called is None
        ]
        return _UnusedResourceTypes(
            type=FunctionIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._never_run_message}. This function has never been called",
            url="/functions",
        )

    def _unused_hosted_extractors(
        self, client: ToolkitClient, requests: _Requests, created_before_epoch: int
    ) -> CheckResult:
        """Hosted extractor jobs that have never run, together with the sources, destinations, and mappings
        not used by any job."""
        hosted = client.hosted_extractors
        jobs_future = requests.submit(hosted.jobs.list, limit=None)
        sources_future = requests.submit(hosted.sources.list, limit=None)
        destinations_future = requests.submit(hosted.destinations.list, limit=None)
        mappings_future = requests.submit(hosted.mappings.list, limit=None)
        jobs = jobs_future.result()
        candidate_jobs = [job for job in jobs if job.created_time < created_before_epoch]
        has_logs = requests.each(lambda job: bool(hosted.jobs.list_logs(job=job.external_id, limit=1)), candidate_jobs)
        results: list[_UnusedResourceTypes | InternalValidatorException] = [
            _UnusedResourceTypes(
                type=HostedExtractorJobIO.as_resource_type(),
                identifiers=[job.as_id() for job, logs in zip(candidate_jobs, has_logs) if not logs],
                message=f"{self._created_before_message} and no logs of this hosted extractor job were found",
                url="/hostedextractors/jobs",
            )
        ]
        used_by_jobs: list[tuple[type[ResourceIO], Future, set[str], str]] = [
            (HostedExtractorSourceIO, sources_future, {job.source_id for job in jobs}, "/hostedextractors/sources"),
            (
                HostedExtractorDestinationIO,
                destinations_future,
                {job.destination_id for job in jobs},
                "/hostedextractors/destinations",
            ),
            (
                HostedExtractorMappingIO,
                mappings_future,
                {job.format.mapping_id for job in jobs if isinstance(job.format, CustomFormat)},
                "/hostedextractors/mappings",
            ),
        ]
        for io_cls, future, used, url in used_by_jobs:
            try:
                resources = future.result()
            except CHECK_ERRORS as e:
                results.append(self._failed_check(client, io_cls, e))
                continue
            results.append(
                _UnusedResourceTypes(
                    type=io_cls.as_resource_type(),
                    identifiers=[
                        ExternalId(external_id=resource.external_id)
                        for resource in resources
                        if resource.created_time < created_before_epoch and resource.external_id not in used
                    ],
                    message=f"{self._created_before_message} and is not used by any hosted extractor job",
                    url=url,
                )
            )
        return results

    def _unused_views(self, client: ToolkitClient, requests: _Requests, created_before_epoch: int) -> CheckResult:
        """Views without instances. Views used for records are skipped, as records are not instances. Views that
        cannot be queried or do not map any containers, for example, views with only edge or reverse direct relation
        properties, cannot have instances of their own, and are skipped as well."""
        views = requests.request(client.views.list, filter=ViewFilter(include_global=False), limit=None)
        candidates = [
            (view.as_id(), instance_type)
            for view in views
            if view.created_time < created_before_epoch
            and view.queryable
            and view.mapped_containers
            and view.used_for in INSTANCE_TYPES_BY_USED_FOR
            for instance_type in INSTANCE_TYPES_BY_USED_FOR[view.used_for]
        ]
        has_instances = requests.each(lambda candidate: self._has_instances(client, *candidate), candidates)
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

    def _unused_data_models(self, client: ToolkitClient, requests: _Requests, created_before_epoch: int) -> CheckResult:
        data_models = requests.request(
            client.data_models.list, filter=DataModelFilter(include_global=False), limit=None
        )
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

    def _unused_extraction_pipelines(
        self, client: ToolkitClient, requests: _Requests, created_before_epoch: int
    ) -> CheckResult:
        unused = [
            pipeline.as_id()
            for pipeline in requests.request(client.extraction_pipelines.list, limit=None)
            if pipeline.created_time < created_before_epoch and pipeline.last_seen is None
        ]
        return _UnusedResourceTypes(
            type=ExtractionPipelineIO.as_resource_type(),
            identifiers=unused,
            message=f"{self._never_run_message}. No extractor has reported a run to this extraction pipeline",
            url="/extpipes",
        )

    def _unused_raw(self, client: ToolkitClient, requests: _Requests, created_before_epoch: int) -> CheckResult:
        """RAW databases without tables and RAW tables without rows."""
        databases = requests.request(client.raw.databases.list, limit=None)
        tables_by_database = requests.each(
            lambda database: client.raw.tables.list(database.name, limit=None), databases
        )
        unused_databases = [
            database.as_id()
            for database, tables in zip(databases, tables_by_database)
            if database.created_time < created_before_epoch and not tables
        ]
        candidate_tables = [
            table for tables in tables_by_database for table in tables if table.created_time < created_before_epoch
        ]
        # Requesting only the row keys avoids downloading the content of the rows.
        has_rows = requests.each(
            lambda table: bool(client.raw.tables.rows.list(table.db_name, table.name, limit=1, columns=[","])),
            candidate_tables,
        )
        return [
            _UnusedResourceTypes(
                type=RawDatabaseIO.as_resource_type(),
                identifiers=unused_databases,
                message=f"{self._created_before_message} and has no tables",
                url="/raw/dbs",
            ),
            _UnusedResourceTypes(
                type=RawTableIO.as_resource_type(),
                identifiers=[table.as_id() for table, has in zip(candidate_tables, has_rows) if not has],
                message=f"{self._created_before_message} and has no rows",
                url="/raw/tables",
            ),
        ]
