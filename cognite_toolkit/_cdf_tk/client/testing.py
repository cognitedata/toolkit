from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.datapoints_subscriptions import DatapointsSubscriptionAPI
from cognite.client._api.functions import FunctionCallsAPI
from cognite.client._api.functions import FunctionSchedulesAPI as LegacyFunctionSchedulesAPI
from cognite.client._api.raw import RawDatabasesAPI as LegacyRawDatabasesAPI
from cognite.client._api.raw import RawRowsAPI
from cognite.client._api.raw import RawTablesAPI as LegacyRawTablesAPI
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client.testing import CogniteClientMock
from rich.console import Console

from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient
from cognite_toolkit._cdf_tk.client.api.hosted_extractors import HostedExtractorsAPI
from cognite_toolkit._cdf_tk.client.api.legacy.canvas import CanvasAPI, IndustrialCanvasAPI
from cognite_toolkit._cdf_tk.client.api.legacy.charts import ChartsAPI
from cognite_toolkit._cdf_tk.client.api.legacy.dml import DMLAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_data_modeling import ExtendedInstancesAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_files import ExtendedFileMetadataAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_functions import ExtendedFunctionsAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_raw import ExtendedRawAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_timeseries import ExtendedTimeSeriesAPI
from cognite_toolkit._cdf_tk.client.api.legacy.search_config import SearchConfigurationsAPI
from cognite_toolkit._cdf_tk.client.api.raw import RawAPI, RawDatabasesAPI, RawTablesAPI
from cognite_toolkit._cdf_tk.client.api.robotics import RoboticsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_capabilities import CapabilitiesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_data_postprocessing import DataPostProcessingAPI
from cognite_toolkit._cdf_tk.client.api.robotics_frames import FramesAPI
from cognite_toolkit._cdf_tk.client.api.robotics_locations import LocationsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_maps import MapsAPI
from cognite_toolkit._cdf_tk.client.api.robotics_robots import RobotsAPI

from ._toolkit_client import ToolAPI
from .api.assets import AssetsAPI
from .api.datasets import DataSetsAPI
from .api.events import EventsAPI
from .api.extraction_pipelines import ExtractionPipelinesAPI
from .api.filemetadata import FileMetadataAPI
from .api.function_schedules import FunctionSchedulesAPI
from .api.functions import FunctionsAPI
from .api.hosted_extractor_destinations import HostedExtractorDestinationsAPI
from .api.hosted_extractor_jobs import HostedExtractorJobsAPI
from .api.hosted_extractor_mappings import HostedExtractorMappingsAPI
from .api.hosted_extractor_sources import HostedExtractorSourcesAPI
from .api.infield import APMConfigAPI, InfieldAPI, InFieldCDMConfigAPI, InfieldConfigAPI
from .api.instances import InstancesAPI
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
    LookupAPI,
    MigrationAPI,
    MigrationLookupAPI,
    ResourceViewMappingAPI,
)
from .api.project import ProjectAPI
from .api.search import SearchAPI
from .api.security_categories import SecurityCategoriesAPI
from .api.sequences import SequencesAPI
from .api.simulator_model_revisions import SimulatorModelRevisionsAPI
from .api.simulator_models import SimulatorModelsAPI
from .api.simulator_routine_revisions import SimulatorRoutineRevisionsAPI
from .api.simulator_routines import SimulatorRoutinesAPI
from .api.simulators import SimulatorsAPI
from .api.streams import StreamsAPI
from .api.three_d import ThreeDAPI, ThreeDClassicModelsAPI
from .api.timeseries import TimeSeriesAPI
from .api.token import TokenAPI
from .api.transformations import TransformationsAPI
from .api.verify import VerifyAPI
from .api.workflow_triggers import WorkflowTriggersAPI
from .api.workflow_versions import WorkflowVersionsAPI
from .api.workflows import WorkflowsAPI


class ToolkitClientMock(CogniteClientMock):
    """Mock for ToolkitClient object

    All APIs are replaced with specked MagicMock objects.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "parent" in kwargs:
            super().__init__(*args, **kwargs)
            return None
        super().__init__(*args, **kwargs)
        self.console = Console()
        # Developer note:
        # - Please add your mocked APIs in chronological order
        # - For nested APIs:
        #   - Add spacing above and below
        #   - Use `spec=MyAPI` only for "top level"
        #   - Use `spec_set=MyNestedAPI` for all nested APIs
        self.canvas = MagicMock(spec=CanvasAPI)
        self.canvas.industrial = MagicMock(spec_set=IndustrialCanvasAPI)
        self.charts = MagicMock(spec_set=ChartsAPI)
        self.files = MagicMock(spec_set=ExtendedFileMetadataAPI)
        self.functions = MagicMock(spec=ExtendedFunctionsAPI)
        self.functions.calls = MagicMock(spec_set=FunctionCallsAPI)
        self.functions.schedules = MagicMock(spec_set=LegacyFunctionSchedulesAPI)
        self.infield = MagicMock(spec=InfieldAPI)
        self.infield.apm_config = MagicMock(spec_set=APMConfigAPI)
        self.infield.config = MagicMock(spec_set=InfieldConfigAPI)
        self.infield.cdm_config = MagicMock(spec_set=InFieldCDMConfigAPI)

        self.project = MagicMock(spec_set=ProjectAPI)

        self.search = MagicMock(spec=SearchAPI)
        self.search.configurations = MagicMock(spec_set=SearchConfigurationsAPI)
        self.dml = MagicMock(spec_set=DMLAPI)
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
        self.migration.resource_view_mapping = MagicMock(spec_set=ResourceViewMappingAPI)
        self.migration.created_source_system = MagicMock(spec_set=CreatedSourceSystemAPI)
        self.raw = MagicMock(spec=ExtendedRawAPI)
        self.raw.databases = MagicMock(spec_set=LegacyRawDatabasesAPI)
        self.raw.rows = MagicMock(spec_set=RawRowsAPI)
        self.raw.tables = MagicMock(spec_set=LegacyRawTablesAPI)

        self.data_modeling.instances = MagicMock(spec_set=ExtendedInstancesAPI)

        self.time_series = MagicMock(spec=ExtendedTimeSeriesAPI)
        self.time_series.data = MagicMock(spec=DatapointsAPI)
        self.time_series.data.synthetic = MagicMock(spec_set=SyntheticDatapointsAPI)
        self.time_series.subscriptions = MagicMock(spec_set=DatapointsSubscriptionAPI)

        self.tool = MagicMock(spec=ToolAPI)
        self.tool.three_d = MagicMock(spec=ThreeDAPI)
        self.tool.three_d.models_classic = MagicMock(spec_set=ThreeDClassicModelsAPI)
        self.tool.assets = MagicMock(spec_set=AssetsAPI)
        self.tool.timeseries = MagicMock(spec_set=TimeSeriesAPI)
        self.tool.filemetadata = MagicMock(spec_set=FileMetadataAPI)
        self.tool.instances = MagicMock(spec=InstancesAPI)
        self.tool.location_filters = MagicMock(spec=LocationFiltersAPI)
        self.tool.events = MagicMock(spec_set=EventsAPI)
        self.tool.functions = MagicMock(spec=FunctionsAPI)
        self.tool.functions.schedules = MagicMock(spec_set=FunctionSchedulesAPI)
        self.tool.simulators = MagicMock(spec=SimulatorsAPI)
        self.tool.simulators.models = MagicMock(spec_set=SimulatorModelsAPI)
        self.tool.simulators.model_revisions = MagicMock(spec_set=SimulatorModelRevisionsAPI)
        self.tool.simulators.routines = MagicMock(spec_set=SimulatorRoutinesAPI)
        self.tool.simulators.routine_revisions = MagicMock(spec_set=SimulatorRoutineRevisionsAPI)
        self.tool.datasets = MagicMock(spec_set=DataSetsAPI)
        self.tool.extraction_pipelines = MagicMock(spec_set=ExtractionPipelinesAPI)
        self.tool.hosted_extractors = MagicMock(spec=HostedExtractorsAPI)
        self.tool.hosted_extractors.sources = MagicMock(spec_set=HostedExtractorSourcesAPI)
        self.tool.hosted_extractors.jobs = MagicMock(spec_set=HostedExtractorJobsAPI)
        self.tool.hosted_extractors.destinations = MagicMock(spec_set=HostedExtractorDestinationsAPI)
        self.tool.hosted_extractors.mappings = MagicMock(spec_set=HostedExtractorMappingsAPI)
        self.tool.labels = MagicMock(spec_set=LabelsAPI)
        self.tool.raw = MagicMock(spec=RawAPI)
        self.tool.raw.databases = MagicMock(spec_set=RawDatabasesAPI)
        self.tool.raw.tables = MagicMock(spec_set=RawTablesAPI)
        self.tool.robotics = MagicMock(spec=RoboticsAPI)
        self.tool.robotics.capabilities = MagicMock(spec_set=CapabilitiesAPI)
        self.tool.robotics.data_postprocessing = MagicMock(spec_set=DataPostProcessingAPI)
        self.tool.robotics.frames = MagicMock(spec_set=FramesAPI)
        self.tool.robotics.locations = MagicMock(spec_set=LocationsAPI)
        self.tool.robotics.maps = MagicMock(spec_set=MapsAPI)
        self.tool.robotics.robots = MagicMock(spec_set=RobotsAPI)
        self.tool.security_categories = MagicMock(spec_set=SecurityCategoriesAPI)
        self.tool.sequences = MagicMock(spec_set=SequencesAPI)
        self.tool.transformations = MagicMock(spec_set=TransformationsAPI)
        self.tool.workflows = MagicMock(spec=WorkflowsAPI)
        self.tool.workflows.triggers = MagicMock(spec_set=WorkflowTriggersAPI)
        self.tool.workflows.versions = MagicMock(spec_set=WorkflowVersionsAPI)

        self.streams = MagicMock(spec=StreamsAPI)

        # This is a helper API, not a real API.
        self.token = TokenAPI(self)
        self.verify = MagicMock(spec_set=VerifyAPI)


@contextmanager
def monkeypatch_toolkit_client() -> Iterator[ToolkitClientMock]:
    toolkit_client_mock = ToolkitClientMock()
    try:
        ToolkitClient.__new__ = lambda *args, **kwargs: toolkit_client_mock  # type: ignore[method-assign]
        yield toolkit_client_mock
    finally:
        ToolkitClient.__new__ = lambda cls, *args, **kwargs: object.__new__(cls)  # type: ignore[method-assign]
