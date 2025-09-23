from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

from cognite.client._api.datapoints import DatapointsAPI
from cognite.client._api.datapoints_subscriptions import DatapointsSubscriptionAPI
from cognite.client._api.functions import FunctionCallsAPI, FunctionSchedulesAPI
from cognite.client._api.raw import RawDatabasesAPI, RawRowsAPI, RawTablesAPI
from cognite.client._api.synthetic_time_series import SyntheticDatapointsAPI
from cognite.client.testing import CogniteClientMock

from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient

from .api.canvas import CanvasAPI, IndustrialCanvasAPI
from .api.charts import ChartsAPI
from .api.dml import DMLAPI
from .api.extended_data_modeling import ExtendedInstancesAPI
from .api.extended_files import ExtendedFileMetadataAPI
from .api.extended_functions import ExtendedFunctionsAPI
from .api.extended_raw import ExtendedRawAPI
from .api.extended_timeseries import ExtendedTimeSeriesAPI
from .api.location_filters import LocationFiltersAPI
from .api.lookup import (
    AssetLookUpAPI,
    DataSetLookUpAPI,
    ExtractionPipelineLookUpAPI,
    FunctionLookUpAPI,
    LocationFiltersLookUpAPI,
    LookUpGroup,
    SecurityCategoriesLookUpAPI,
    TimeSeriesLookUpAPI,
)
from .api.migration import InstanceSourceAPI, MigrationAPI, ResourceViewMappingAPI
from .api.robotics import RoboticsAPI
from .api.robotics.capabilities import CapabilitiesAPI
from .api.robotics.data_postprocessing import DataPostProcessingAPI
from .api.robotics.frames import FramesAPI
from .api.robotics.locations import LocationsAPI as RoboticsLocationsAPI
from .api.robotics.maps import MapsAPI
from .api.search import SearchAPI
from .api.search_config import SearchConfigurationsAPI
from .api.token import TokenAPI
from .api.verify import VerifyAPI


class ToolkitClientMock(CogniteClientMock):
    """Mock for ToolkitClient object

    All APIs are replaced with specked MagicMock objects.
    """

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        if "parent" in kwargs:
            super().__init__(*args, **kwargs)
            return None
        super().__init__(*args, **kwargs)
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
        self.functions.schedules = MagicMock(spec_set=FunctionSchedulesAPI)

        self.search = MagicMock(spec=SearchAPI)
        self.search.locations = MagicMock(spec_set=LocationFiltersAPI)
        self.search.configurations = MagicMock(spec_set=SearchConfigurationsAPI)
        self.dml = MagicMock(spec_set=DMLAPI)
        self.lookup = MagicMock(spec=LookUpGroup)
        self.lookup.data_sets = MagicMock(spec_set=DataSetLookUpAPI)
        self.lookup.assets = MagicMock(spec_set=AssetLookUpAPI)
        self.lookup.time_series = MagicMock(spec_set=TimeSeriesLookUpAPI)
        self.lookup.security_categories = MagicMock(spec_set=SecurityCategoriesLookUpAPI)
        self.lookup.location_filters = MagicMock(spec_set=LocationFiltersLookUpAPI)
        self.lookup.extraction_pipelines = MagicMock(spec_set=ExtractionPipelineLookUpAPI)
        self.lookup.functions = MagicMock(spec_set=FunctionLookUpAPI)
        self.migration = MagicMock(spec=MigrationAPI)
        self.migration.instance_source = MagicMock(spec_set=InstanceSourceAPI)
        self.migration.resource_view_mapping = MagicMock(spec_set=ResourceViewMappingAPI)
        self.raw = MagicMock(spec=ExtendedRawAPI)
        self.raw.databases = MagicMock(spec_set=RawDatabasesAPI)
        self.raw.rows = MagicMock(spec_set=RawRowsAPI)
        self.raw.tables = MagicMock(spec_set=RawTablesAPI)

        self.robotics = MagicMock()
        self.robotics.robots = MagicMock(spec=RoboticsAPI)
        self.robotics.data_postprocessing = MagicMock(spec_set=DataPostProcessingAPI)
        self.robotics.locations = MagicMock(spec_set=RoboticsLocationsAPI)
        self.robotics.frames = MagicMock(spec_set=FramesAPI)
        self.robotics.maps = MagicMock(spec_set=MapsAPI)
        self.robotics.capabilities = MagicMock(spec_set=CapabilitiesAPI)

        self.data_modeling.instances = MagicMock(spec_set=ExtendedInstancesAPI)

        self.time_series = MagicMock(spec=ExtendedTimeSeriesAPI)
        self.time_series.data = MagicMock(spec=DatapointsAPI)
        self.time_series.data.synthetic = MagicMock(spec_set=SyntheticDatapointsAPI)
        self.time_series.subscriptions = MagicMock(spec_set=DatapointsSubscriptionAPI)

        # This is a helper API, not a real API.
        self.token = TokenAPI(self)
        self.verify = MagicMock(spec_set=VerifyAPI)


@contextmanager
def monkeypatch_toolkit_client() -> Iterator[ToolkitClientMock]:
    toolkit_client_mock = ToolkitClientMock()
    ToolkitClient.__new__ = lambda *args, **kwargs: toolkit_client_mock  # type: ignore[method-assign]
    yield toolkit_client_mock
    ToolkitClient.__new__ = lambda cls, *args, **kwargs: object.__new__(cls)  # type: ignore[method-assign]
