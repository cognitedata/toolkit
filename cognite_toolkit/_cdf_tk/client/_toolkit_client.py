from typing import cast

from cognite.client import CogniteClient
from rich.console import Console

from cognite_toolkit._cdf_tk.utils.http_client import HTTPClient

from .api.canvas import CanvasAPI
from .api.charts import ChartsAPI
from .api.dml import DMLAPI
from .api.extended_data_modeling import ExtendedDataModelingAPI
from .api.extended_files import ExtendedFileMetadataAPI
from .api.extended_functions import ExtendedFunctionsAPI
from .api.extended_raw import ExtendedRawAPI
from .api.extended_timeseries import ExtendedTimeSeriesAPI
from .api.infield import InfieldAPI
from .api.lookup import LookUpGroup
from .api.migration import MigrationAPI
from .api.project import ProjectAPI
from .api.robotics import RoboticsAPI
from .api.search import SearchAPI
from .api.token import TokenAPI
from .api.verify import VerifyAPI
from .config import ToolkitClientConfig


class ToolkitClient(CogniteClient):
    def __init__(self, config: ToolkitClientConfig | None = None, enable_set_pending_ids: bool = False) -> None:
        super().__init__(config=config)
        http_client = HTTPClient(self.config)
        toolkit_config = ToolkitClientConfig.from_client_config(self.config)
        self.console = Console()
        self.search = SearchAPI(self._config, self._API_VERSION, self)
        self.robotics = RoboticsAPI(self._config, self._API_VERSION, self)
        self.dml = DMLAPI(self._config, self._API_VERSION, self)
        self.verify = VerifyAPI(self._config, self._API_VERSION, self)
        self.lookup = LookUpGroup(self._config, self._API_VERSION, self, self.console)
        self.functions: ExtendedFunctionsAPI = ExtendedFunctionsAPI(toolkit_config, self._API_VERSION, self)
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

    @property
    def config(self) -> ToolkitClientConfig:
        """Returns a config object containing the configuration for the current client.

        Returns:
            ToolkitClientConfig: The configuration object.
        """
        return cast(ToolkitClientConfig, self._config)
