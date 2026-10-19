from collections.abc import Generator
from contextlib import contextmanager
from typing import Any, cast
from unittest.mock import MagicMock

from rich.console import Console

from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient
from cognite_toolkit._cdf_tk.client.api.canvas import IndustrialCanvasAPI
from cognite_toolkit._cdf_tk.client.api.charts import ChartsAPI
from cognite_toolkit._cdf_tk.client.api.cognite_files import CogniteFilesAPI
from cognite_toolkit._cdf_tk.client.api.containers import ContainersAPI
from cognite_toolkit._cdf_tk.client.api.data_models import DataModelsAPI
from cognite_toolkit._cdf_tk.client.api.graphql_data_models import GraphQLDataModelsAPI
from cognite_toolkit._cdf_tk.client.api.hosted_extractors import HostedExtractorsAPI
from cognite_toolkit._cdf_tk.client.api.raw import RawAPI, RawDatabasesAPI, RawRowsAPI, RawTablesAPI
from cognite_toolkit._cdf_tk.client.api.search_config import SearchConfigurationsAPI
from cognite_toolkit._cdf_tk.client.api.signal_sinks import SignalSinksAPI
from cognite_toolkit._cdf_tk.client.api.signal_subscriptions import SignalSubscriptionsAPI
from cognite_toolkit._cdf_tk.client.api.spaces import SpacesAPI
from cognite_toolkit._cdf_tk.client.api.transformation_externaldata import TransformationExternalDataSourcesAPI
from cognite_toolkit._cdf_tk.client.api.transformation_jobs import TransformationJobsAPI
from cognite_toolkit._cdf_tk.client.api.transformation_notifications import TransformationNotificationsAPI
from cognite_toolkit._cdf_tk.client.api.transformation_schedules import TransformationSchedulesAPI
from cognite_toolkit._cdf_tk.client.api.views import ViewsAPI

from . import ToolkitClientConfig
from .api.agents import AgentsAPI
from .api.app_versions import AppVersionsAPI
from .api.apps import AppsAPI
from .api.assets import AssetsAPI
from .api.chart_scheduled_calculations import ChartScheduledCalculationsAPI
from .api.charts_monitoring_job import ChartMonitoringJobsAPI
from .api.data_product_versions import DataProductVersionsAPI
from .api.data_products import DataProductsAPI
from .api.datapoint_subscription import DatapointSubscriptionsAPI
from .api.datapoints import DatapointsAPI
from .api.datasets import DataSetsAPI
from .api.documents import DocumentsAPI
from .api.events import EventsAPI
from .api.extraction_pipeline_config import ExtractionPipelineConfigsAPI
from .api.extraction_pipelines import ExtractionPipelinesAPI
from .api.filemetadata import FileMetadataAPI
from .api.function_calls import FunctionCallsAPI
from .api.function_schedules import FunctionSchedulesAPI
from .api.functions import FunctionsAPI
from .api.groups import GroupsAPI
from .api.hosted_extractor_destinations import HostedExtractorDestinationsAPI
from .api.hosted_extractor_jobs import HostedExtractorJobsAPI
from .api.hosted_extractor_mappings import HostedExtractorMappingsAPI
from .api.hosted_extractor_sources import HostedExtractorSourcesAPI
from .api.infield import APMConfigAPI, InfieldAPI, InFieldCDMConfigAPI, InfieldConfigAPI
from .api.instances import InstancesAPI
from .api.integrations import (
    IntegrationActionsAPI,
    IntegrationConfigurationAPI,
    IntegrationErrorsAPI,
    IntegrationsAPI,
    IntegrationTasksAPI,
)
from .api.labels import LabelsAPI
from .api.location_filters import LocationFiltersAPI
from .api.lookup import (
    AssetLookUpAPI,
    DataSetLookUpAPI,
    EventLookUpAPI,
    ExtractionPipelineLookUpAPI,
    FileMetadataLookUpAPI,
    FunctionLookUpAPI,
    LocationFiltersLookUpAPI,
    LookUpGroup,
    SecurityCategoriesLookUpAPI,
    TimeSeriesLookUpAPI,
)
from .api.migration import (
    CreatedSourceSystemAPI,
    InstanceSourceAPI,
    InstanceSpaceRelocationSourceAPI,
    LookupAPI,
    MigrationAPI,
    MigrationLookupAPI,
    ResourceViewMappingsAPI,
)
from .api.principals import PrincipalsAPI
from .api.project import ProjectAPI
from .api.records import RecordsAPI
from .api.relationships import RelationshipsAPI
from .api.ruleset_versions import RuleSetVersionsAPI
from .api.rulesets import RuleSetsAPI
from .api.sap_writeback import SAPEndpointsAPI, SAPInstancesAPI, SAPWritebackAPI, SchemaMappingsAPI
from .api.security_categories import SecurityCategoriesAPI
from .api.sequence_rows import SequenceRowsAPI
from .api.sequences import SequencesAPI
from .api.sessions import SessionAPI
from .api.simulator_model_revisions import SimulatorModelRevisionsAPI
from .api.simulator_models import SimulatorModelsAPI
from .api.simulator_routine_revisions import SimulatorRoutineRevisionsAPI
from .api.simulator_routines import SimulatorRoutinesAPI
from .api.simulators import SimulatorsAPI
from .api.skills import SkillsAPI
from .api.statistics import SpaceStatisticsAPI, StatisticsAPI
from .api.streamlit_ import StreamlitAPI
from .api.streams import StreamsAPI
from .api.three_d import (
    ThreeDAPI,
    ThreeDClassicAssetMappingAPI,
    ThreeDClassicModelsAPI,
    ThreeDClassicRevisionsAPI,
    ThreeDDMAssetMappingAPI,
    ThreeDNodesAPI,
)
from .api.timeseries import TimeSeriesAPI
from .api.token import ToolkitTokenAPI
from .api.transformations import TransformationsAPI
from .api.user_profiles import UserProfilesAPI
from .api.workflow_executions import WorkflowExecutionsAPI
from .api.workflow_triggers import WorkflowTriggersAPI
from .api.workflow_versions import WorkflowVersionsAPI
from .api.workflows import WorkflowsAPI


class ToolkitClientMock(ToolkitClient):
    """Mock for ToolkitClient object

    All APIs are replaced with specked MagicMock objects.
    """

    # These attributes are declared as ``MagicMock`` (overriding the real API types inherited
    # from ``ToolkitClient``) so that type checkers understand that a ``ToolkitClientMock`` *is* a
    # ``ToolkitClient`` while still allowing tests to configure them with ``.return_value`` /
    # ``.side_effect`` etc. Keep this list in sync with the attributes assigned in ``__init__``.
    canvas: MagicMock
    charts: MagicMock
    infield: MagicMock
    project: MagicMock
    lookup: MagicMock
    migration: MagicMock
    principals: MagicMock
    sessions: MagicMock
    agents: MagicMock
    skills: MagicMock
    apps: MagicMock
    datapoint_subscriptions: MagicMock
    three_d: MagicMock
    assets: MagicMock
    cognite_files: MagicMock
    timeseries: MagicMock
    filemetadata: MagicMock
    instances: MagicMock
    spaces: MagicMock
    views: MagicMock
    containers: MagicMock
    data_models: MagicMock
    graphql_data_models: MagicMock
    location_filters: MagicMock
    events: MagicMock
    functions: MagicMock
    groups: MagicMock
    search_configurations: MagicMock
    signal_sinks: MagicMock
    signal_subscriptions: MagicMock
    simulators: MagicMock
    datasets: MagicMock
    documents: MagicMock
    extraction_pipelines: MagicMock
    hosted_extractors: MagicMock
    labels: MagicMock
    raw: MagicMock
    relationships: MagicMock
    security_categories: MagicMock
    sequences: MagicMock
    transformations: MagicMock
    streamlit: MagicMock
    token: MagicMock
    workflows: MagicMock
    data_products: MagicMock
    rulesets: MagicMock
    records: MagicMock
    statistics: MagicMock
    streams: MagicMock
    sap_writeback: MagicMock
    integrations: MagicMock
    user_profiles: MagicMock

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        # Intentionally do NOT call super().__init__(): it would require a real
        # ToolkitClientConfig and build real API clients. We replace every API
        # attribute with a MagicMock below.
        # Guard against re-initialization: ``monkeypatch_toolkit_client`` patches
        # ``ToolkitClient.__new__`` to return this singleton. Any code that constructs a
        # ``ToolkitClient(...)`` (e.g. TransformationIO building a session client for OIDC
        # credentials) would otherwise trigger ``__init__`` again and wipe out all the mock
        # return values/side effects configured by the test.
        if getattr(self, "_is_initialized", False):
            return
        self._is_initialized = True
        self.console = Console()
        # spec= (not spec_set): ClientConfig fields are not plain class attributes, so spec_set
        # blocked assigning project/timeout in tests. Defaults keep HTTPClient(...) usable.
        self.config = MagicMock(spec=ToolkitClientConfig)
        self.config.client_name = "toolkit-test-mock"
        self.config.project = "pytest-project"
        self.config.base_url = "https://bluefield.cognitedata.com"
        self.config.cdf_cluster = "bluefield"
        self.config.timeout = 30
        self.config.file_transfer_timeout = None
        self.config.api_subversion = None
        self.config.debug = False
        self.config.is_strict_validation = True
        self.config.headers = None
        _credentials = MagicMock()
        _credentials.authorization_header.return_value = ("Authorization", "Bearer mock-token")
        self.config.credentials = _credentials
        # Developer note:
        # - Please add your mocked APIs in chronological order
        # - For nested APIs:
        #   - Add spacing above and below
        #   - Use `spec=MyAPI` only for "top level"
        #   - Use `spec_set=MyNestedAPI` for all nested APIs
        self.canvas = MagicMock(spec_set=IndustrialCanvasAPI)
        self.charts = MagicMock(spec=ChartsAPI)
        self.charts.monitoring_jobs = MagicMock(spec_set=ChartMonitoringJobsAPI)
        self.charts.scheduled_calculations = MagicMock(spec_set=ChartScheduledCalculationsAPI)
        self.infield = MagicMock(spec=InfieldAPI)
        self.infield.apm_config = MagicMock(spec_set=APMConfigAPI)
        self.infield.config = MagicMock(spec_set=InfieldConfigAPI)
        self.infield.cdm_config = MagicMock(spec_set=InFieldCDMConfigAPI)

        self.project = MagicMock(spec_set=ProjectAPI)

        self.lookup = MagicMock(spec=LookUpGroup)
        self.lookup.data_sets = MagicMock(spec_set=DataSetLookUpAPI)
        self.lookup.assets = MagicMock(spec_set=AssetLookUpAPI)
        self.lookup.time_series = MagicMock(spec_set=TimeSeriesLookUpAPI)
        self.lookup.files = MagicMock(spec_set=FileMetadataLookUpAPI)
        self.lookup.events = MagicMock(spec_set=EventLookUpAPI)
        self.lookup.security_categories = MagicMock(spec_set=SecurityCategoriesLookUpAPI)
        self.lookup.location_filters = MagicMock(spec_set=LocationFiltersLookUpAPI)
        self.lookup.extraction_pipelines = MagicMock(spec_set=ExtractionPipelineLookUpAPI)
        self.lookup.functions = MagicMock(spec_set=FunctionLookUpAPI)
        self.migration = MagicMock(spec=MigrationAPI)
        self.migration.instance_source = MagicMock(spec_set=InstanceSourceAPI)
        self.migration.lookup = MagicMock(spec=MigrationLookupAPI)
        self.migration.lookup.assets = MagicMock(spec_set=LookupAPI)
        self.migration.lookup.events = MagicMock(spec_set=LookupAPI)
        self.migration.lookup.files = MagicMock(spec_set=LookupAPI)
        self.migration.lookup.time_series = MagicMock(spec_set=LookupAPI)
        self.migration.resource_view_mapping = MagicMock(spec_set=ResourceViewMappingsAPI)
        self.migration.created_source_system = MagicMock(spec_set=CreatedSourceSystemAPI)
        self.migration.instance_space_relocation_source = MagicMock(spec_set=InstanceSpaceRelocationSourceAPI)
        self.migration.instance_space_relocation_source.retrieve.return_value = []
        self.principals = MagicMock(spec_set=PrincipalsAPI)
        self.sessions = MagicMock(spec_set=SessionAPI)

        def create_single(item: Any) -> Any:
            # SessionAPI.create_single delegates to create. Keep that so tests that stub
            # sessions.create still produce a real session response.
            return self.sessions.create([item])[0]

        self.sessions.create_single.side_effect = create_single

        self.agents = MagicMock(spec=AgentsAPI)
        self.skills = MagicMock(spec=SkillsAPI)
        self.apps = MagicMock(spec=AppsAPI)
        self.apps.versions = MagicMock(spec=AppVersionsAPI)
        self.datapoint_subscriptions = MagicMock(spec=DatapointSubscriptionsAPI)
        self.three_d = MagicMock(spec=ThreeDAPI)
        self.three_d.models_classic = MagicMock(spec_set=ThreeDClassicModelsAPI)
        self.three_d.revisions_classic = MagicMock(spec_set=ThreeDClassicRevisionsAPI)
        self.three_d.nodes = MagicMock(spec_set=ThreeDNodesAPI)
        self.three_d.asset_mappings_classic = MagicMock(spec_set=ThreeDClassicAssetMappingAPI)
        self.three_d.asset_mappings_dm = MagicMock(spec_set=ThreeDDMAssetMappingAPI)
        self.assets = MagicMock(spec_set=AssetsAPI)
        self.cognite_files = MagicMock(spec_set=CogniteFilesAPI)
        self.timeseries = MagicMock(spec=TimeSeriesAPI)
        self.timeseries.datapoints = MagicMock(spec_set=DatapointsAPI)
        self.filemetadata = MagicMock(spec_set=FileMetadataAPI)
        self.instances = MagicMock(spec=InstancesAPI)
        self.spaces = MagicMock(spec=SpacesAPI)
        self.views = MagicMock(spec=ViewsAPI)
        self.containers = MagicMock(spec=ContainersAPI)
        self.data_models = MagicMock(spec=DataModelsAPI)
        self.graphql_data_models = MagicMock(spec=GraphQLDataModelsAPI)
        self.location_filters = MagicMock(spec=LocationFiltersAPI)
        self.events = MagicMock(spec_set=EventsAPI)
        self.functions = MagicMock(spec=FunctionsAPI)
        self.functions.calls = MagicMock(spec_set=FunctionCallsAPI)
        self.functions.schedules = MagicMock(spec_set=FunctionSchedulesAPI)
        self.groups = MagicMock(spec_set=GroupsAPI)
        self.search_configurations = MagicMock(spec_set=SearchConfigurationsAPI)
        self.signal_sinks = MagicMock(spec_set=SignalSinksAPI)
        self.signal_subscriptions = MagicMock(spec_set=SignalSubscriptionsAPI)
        self.simulators = MagicMock(spec=SimulatorsAPI)
        self.simulators.models = MagicMock(spec_set=SimulatorModelsAPI)
        self.simulators.model_revisions = MagicMock(spec_set=SimulatorModelRevisionsAPI)
        self.simulators.routines = MagicMock(spec_set=SimulatorRoutinesAPI)
        self.simulators.routine_revisions = MagicMock(spec_set=SimulatorRoutineRevisionsAPI)
        self.datasets = MagicMock(spec_set=DataSetsAPI)
        self.documents = MagicMock(spec_set=DocumentsAPI)
        self.extraction_pipelines = MagicMock(spec=ExtractionPipelinesAPI)
        self.extraction_pipelines.configs = MagicMock(spec_set=ExtractionPipelineConfigsAPI)
        self.hosted_extractors = MagicMock(spec=HostedExtractorsAPI)
        self.hosted_extractors.sources = MagicMock(spec_set=HostedExtractorSourcesAPI)
        self.hosted_extractors.jobs = MagicMock(spec_set=HostedExtractorJobsAPI)
        self.hosted_extractors.destinations = MagicMock(spec_set=HostedExtractorDestinationsAPI)
        self.hosted_extractors.mappings = MagicMock(spec_set=HostedExtractorMappingsAPI)
        self.labels = MagicMock(spec_set=LabelsAPI)
        self.raw = MagicMock(spec=RawAPI)
        self.raw.databases = MagicMock(spec_set=RawDatabasesAPI)
        self.raw.tables = MagicMock(spec=RawTablesAPI)
        self.raw.tables.rows = MagicMock(spec_set=RawRowsAPI)
        self.relationships = MagicMock(spec_set=RelationshipsAPI)
        self.security_categories = MagicMock(spec_set=SecurityCategoriesAPI)
        self.sequences = MagicMock(spec=SequencesAPI)
        self.sequences.rows = MagicMock(spec_set=SequenceRowsAPI)
        self.transformations = MagicMock(spec=TransformationsAPI)
        self.streamlit = MagicMock(spec=StreamlitAPI)
        self.token = MagicMock(spec=ToolkitTokenAPI)
        self.transformations.schedules = MagicMock(spec=TransformationSchedulesAPI)
        self.transformations.notifications = MagicMock(spec=TransformationNotificationsAPI)
        self.transformations.jobs = MagicMock(spec=TransformationJobsAPI)
        self.transformations.external_data_sources = MagicMock(spec=TransformationExternalDataSourcesAPI)
        self.workflows = MagicMock(spec=WorkflowsAPI)
        self.workflows.triggers = MagicMock(spec_set=WorkflowTriggersAPI)
        self.workflows.versions = MagicMock(spec_set=WorkflowVersionsAPI)
        self.workflows.executions = MagicMock(spec_set=WorkflowExecutionsAPI)
        self.data_products = MagicMock(spec=DataProductsAPI)
        self.data_products.versions = MagicMock(spec_set=DataProductVersionsAPI)
        self.rulesets = MagicMock(spec=RuleSetsAPI)
        self.rulesets.versions = MagicMock(spec_set=RuleSetVersionsAPI)

        self.records = MagicMock(spec=RecordsAPI)
        self.statistics = MagicMock(spec=StatisticsAPI)
        self.statistics.spaces = MagicMock(spec_set=SpaceStatisticsAPI)
        self.streams = MagicMock(spec=StreamsAPI)
        self.sap_writeback = MagicMock(spec=SAPWritebackAPI)
        self.sap_writeback.instances = MagicMock(spec_set=SAPInstancesAPI)
        self.sap_writeback.endpoints = MagicMock(spec_set=SAPEndpointsAPI)
        self.sap_writeback.mappings = MagicMock(spec_set=SchemaMappingsAPI)
        self.integrations = MagicMock(spec=IntegrationsAPI)
        self.integrations.tasks = MagicMock(spec_set=IntegrationTasksAPI)
        self.integrations.actions = MagicMock(spec_set=IntegrationActionsAPI)
        self.integrations.configuration = MagicMock(spec_set=IntegrationConfigurationAPI)
        self.integrations.errors = MagicMock(spec_set=IntegrationErrorsAPI)
        self.user_profiles = MagicMock(spec_set=UserProfilesAPI)


@contextmanager
def monkeypatch_toolkit_client() -> Generator[ToolkitClientMock, None, None]:
    toolkit_client_mock = ToolkitClientMock()
    try:
        ToolkitClient.__new__ = cast(Any, lambda *args, **kwargs: toolkit_client_mock)  # type: ignore[method-assign]
        yield toolkit_client_mock
    finally:
        ToolkitClient.__new__ = cast(  # type: ignore[method-assign]
            Any, lambda cls, *args, **kwargs: object.__new__(cls)
        )
