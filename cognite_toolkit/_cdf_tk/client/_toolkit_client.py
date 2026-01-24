from typing import cast

from cognite.client import CogniteClient
from rich.console import Console

from cognite_toolkit._cdf_tk.client.api.legacy.canvas import CanvasAPI
from cognite_toolkit._cdf_tk.client.api.legacy.charts import ChartsAPI
from cognite_toolkit._cdf_tk.client.api.legacy.dml import DMLAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_data_modeling import ExtendedDataModelingAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_files import ExtendedFileMetadataAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_functions import ExtendedFunctionsAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_raw import ExtendedRawAPI
from cognite_toolkit._cdf_tk.client.api.legacy.extended_timeseries import ExtendedTimeSeriesAPI
from cognite_toolkit._cdf_tk.client.api.legacy.robotics import RoboticsAPI as RoboticsLegacyAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient

from .api.assets import AssetsAPI
from .api.datasets import DataSetsAPI
from .api.events import EventsAPI
from .api.extraction_pipelines import ExtractionPipelinesAPI
from .api.filemetadata import FileMetadataAPI
from .api.hosted_extractors import HostedExtractorsAPI
from .api.infield import InfieldAPI
from .api.instances import InstancesAPI
from .api.labels import LabelsAPI
from .api.lookup import LookUpGroup
from .api.migration import MigrationAPI
from .api.project import ProjectAPI
from .api.raw import RawAPI
from .api.robotics import RoboticsAPI
from .api.search import SearchAPI
from .api.security_categories import SecurityCategoriesAPI
from .api.sequences import SequencesAPI
from .api.simulators import SimulatorsAPI
from .api.streams import StreamsAPI
from .api.three_d import ThreeDAPI
from .api.timeseries import TimeSeriesAPI
from .api.token import TokenAPI
from .api.transformations import TransformationsAPI
from .api.verify import VerifyAPI
from .api.workflows import WorkflowsAPI
from .config import ToolkitClientConfig


class ToolAPI:
    """This is reimplemented CogniteAPIs in Toolkit"""

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self.http_client = http_client
        self.assets = AssetsAPI(http_client)
        self.datasets = DataSetsAPI(http_client)
        self.events = EventsAPI(http_client)
        self.extraction_pipelines = ExtractionPipelinesAPI(http_client)
        self.hosted_extractors = HostedExtractorsAPI(http_client)
        self.instances = InstancesAPI(http_client)
        self.labels = LabelsAPI(http_client)
        self.filemetadata = FileMetadataAPI(http_client)
        self.raw = RawAPI(http_client)
        self.robotics = RoboticsAPI(http_client)
        self.security_categories = SecurityCategoriesAPI(http_client)
        self.sequences = SequencesAPI(http_client)
        self.simulators = SimulatorsAPI(http_client)
        self.three_d = ThreeDAPI(http_client)
        self.timeseries = TimeSeriesAPI(http_client)
        self.transformations = TransformationsAPI(http_client)
        self.workflows = WorkflowsAPI(http_client)


class ToolkitClient(CogniteClient):
    def __init__(
        self,
        config: ToolkitClientConfig | None = None,
        enable_set_pending_ids: bool = False,
        console: Console | None = None,
    ) -> None:
        super().__init__(config=config)
        http_client = HTTPClient(self.config)
        self.http_client = http_client
        toolkit_config = ToolkitClientConfig.from_client_config(self.config)
        self.console = console or Console()
        self.tool = ToolAPI(http_client, self.console)
        self.search = SearchAPI(self._config, self._API_VERSION, self)
        self.robotics = RoboticsLegacyAPI(self._config, self._API_VERSION, self)
        self.dml = DMLAPI(self._config, self._API_VERSION, self)
        self.verify = VerifyAPI(self._config, self._API_VERSION, self)
        self.lookup = LookUpGroup(self._config, self._API_VERSION, self, self.console)
        self.functions: ExtendedFunctionsAPI = ExtendedFunctionsAPI(
            toolkit_config, self._API_VERSION, self, self.console
        )
        self.data_modeling: ExtendedDataModelingAPI = ExtendedDataModelingAPI(self._config, self._API_VERSION, self)
        if enable_set_pending_ids:
            self.time_series: ExtendedTimeSeriesAPI = ExtendedTimeSeriesAPI(self._config, self._API_VERSION, self)
            self.files: ExtendedFileMetadataAPI = ExtendedFileMetadataAPI(self._config, self._API_VERSION, self)
        self.raw: ExtendedRawAPI = ExtendedRawAPI(self._config, self._API_VERSION, self)
        self.canvas = CanvasAPI(self.data_modeling.instances)
        self.migration = MigrationAPI(self.data_modeling.instances)
        self.token = TokenAPI(self)
        self.charts = ChartsAPI(self._config, self._API_VERSION, self)
        self.project = ProjectAPI(config=toolkit_config, cognite_client=self)
        self.infield = InfieldAPI(http_client, self.console)
        self.streams = StreamsAPI(http_client)

    @property
    def config(self) -> ToolkitClientConfig:
        """Returns a config object containing the configuration for the current client.

        Returns:
            ToolkitClientConfig: The configuration object.
        """
        return cast(ToolkitClientConfig, self._config)
