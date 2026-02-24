from typing import cast

from cognite.client import CogniteClient
from rich.console import Console

from cognite_toolkit._cdf_tk.client.api.charts import ChartsAPI
from cognite_toolkit._cdf_tk.client.api.legacy.canvas import CanvasAPI
from cognite_toolkit._cdf_tk.client.api.location_filters import LocationFiltersAPI
from cognite_toolkit._cdf_tk.client.http_client import HTTPClient

from .api.agents import AgentsAPI
from .api.assets import AssetsAPI
from .api.cognite_files import CogniteFilesAPI
from .api.containers import ContainersAPI
from .api.data_models import DataModelsAPI
from .api.data_products import DataProductsAPI
from .api.datapoint_subscription import DatapointSubscriptionsAPI
from .api.datasets import DataSetsAPI
from .api.events import EventsAPI
from .api.extraction_pipelines import ExtractionPipelinesAPI
from .api.filemetadata import FileMetadataAPI
from .api.functions import FunctionsAPI
from .api.graphql_data_models import GraphQLDataModelsAPI
from .api.groups import GroupsAPI
from .api.hosted_extractors import HostedExtractorsAPI
from .api.infield import InfieldAPI
from .api.instances import InstancesAPI
from .api.labels import LabelsAPI
from .api.lookup import LookUpGroup
from .api.migration import MigrationAPI
from .api.project import ProjectAPI
from .api.raw import RawAPI
from .api.relationships import RelationshipsAPI
from .api.robotics import RoboticsAPI
from .api.search_config import SearchConfigurationsAPI
from .api.security_categories import SecurityCategoriesAPI
from .api.sequences import SequencesAPI
from .api.simulators import SimulatorsAPI
from .api.spaces import SpacesAPI
from .api.streamlit_ import StreamlitAPI
from .api.streams import StreamsAPI
from .api.three_d import ThreeDAPI
from .api.timeseries import TimeSeriesAPI
from .api.token import TokenAPI
from .api.transformations import TransformationsAPI
from .api.verify import VerifyAPI
from .api.views import ViewsAPI
from .api.workflows import WorkflowsAPI
from .config import ToolkitClientConfig


class ToolAPI:
    """This is reimplemented CogniteAPIs in Toolkit"""

    def __init__(self, http_client: HTTPClient, console: Console) -> None:
        self.http_client = http_client
        self.agents = AgentsAPI(http_client)
        self.assets = AssetsAPI(http_client)
        self.cognite_files = CogniteFilesAPI(http_client)
        self.datasets = DataSetsAPI(http_client)
        self.datapoint_subscriptions = DatapointSubscriptionsAPI(http_client)
        self.events = EventsAPI(http_client)
        self.extraction_pipelines = ExtractionPipelinesAPI(http_client)
        self.functions = FunctionsAPI(http_client)
        self.groups = GroupsAPI(http_client)
        self.hosted_extractors = HostedExtractorsAPI(http_client)
        self.instances = InstancesAPI(http_client)
        self.spaces = SpacesAPI(http_client)
        self.views = ViewsAPI(http_client)
        self.containers = ContainersAPI(http_client)
        self.data_models = DataModelsAPI(http_client)
        self.graphql_data_models = GraphQLDataModelsAPI(http_client)
        self.labels = LabelsAPI(http_client)
        self.location_filters = LocationFiltersAPI(http_client)
        self.filemetadata = FileMetadataAPI(http_client)
        self.raw = RawAPI(http_client)
        self.robotics = RoboticsAPI(http_client)
        self.security_categories = SecurityCategoriesAPI(http_client)
        self.relationships = RelationshipsAPI(http_client)
        self.sequences = SequencesAPI(http_client)
        self.search_configurations = SearchConfigurationsAPI(http_client)
        self.simulators = SimulatorsAPI(http_client)
        self.three_d = ThreeDAPI(http_client)
        self.timeseries = TimeSeriesAPI(http_client)
        self.transformations = TransformationsAPI(http_client)
        self.workflows = WorkflowsAPI(http_client)
        self.data_products = DataProductsAPI(http_client)
        self.streamlit = StreamlitAPI(http_client)


class ToolkitClient(CogniteClient):
    def __init__(
        self,
        config: ToolkitClientConfig | None = None,
        console: Console | None = None,
    ) -> None:
        super().__init__(config=config)
        http_client = HTTPClient(self.config, console=console)
        self.http_client = http_client
        toolkit_config = ToolkitClientConfig.from_client_config(self.config)
        self.console = console or Console(markup=True)
        self.tool = ToolAPI(http_client, self.console)

        self.verify = VerifyAPI(self._config, self._API_VERSION, self)
        self.lookup = LookUpGroup(self._config, self._API_VERSION, self, self.console)
        self.canvas = CanvasAPI(self.data_modeling.instances)
        self.migration = MigrationAPI(self.data_modeling.instances, http_client)
        self.token = TokenAPI(self)
        self.charts = ChartsAPI(http_client)
        self.project = ProjectAPI(config=toolkit_config, cognite_client=self)
        self.infield = InfieldAPI(http_client)
        self.streams = StreamsAPI(http_client)

    @property
    def config(self) -> ToolkitClientConfig:
        """Returns a config object containing the configuration for the current client.

        Returns:
            ToolkitClientConfig: The configuration object.
        """
        return cast(ToolkitClientConfig, self._config)
