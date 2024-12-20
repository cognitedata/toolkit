from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any
from unittest.mock import MagicMock

from cognite.client.testing import CogniteClientMock

from cognite_toolkit._cdf_tk.client._toolkit_client import ToolkitClient

from .api.dml import DMLAPI
from .api.location_filters import LocationFiltersAPI
from .api.lookup import (
    AssetLookUpAPI,
    DataSetLookUpAPI,
    ExtractionPipelineLookUpAPI,
    LocationFiltersLookUpAPI,
    LookUpGroup,
    SecurityCategoriesLookUpAPI,
    TimeSeriesLookUpAPI,
)
from .api.robotics import RoboticsAPI
from .api.robotics.capabilities import CapabilitiesAPI
from .api.robotics.data_postprocessing import DataPostProcessingAPI
from .api.robotics.frames import FramesAPI
from .api.robotics.locations import LocationsAPI as RoboticsLocationsAPI
from .api.robotics.maps import MapsAPI
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

        self.dml = MagicMock(spec_set=DMLAPI)
        self.location_filters = MagicMock(spec_set=LocationFiltersAPI)
        self.lookup = MagicMock(spec=LookUpGroup)
        self.lookup.data_sets = MagicMock(spec_set=DataSetLookUpAPI)
        self.lookup.assets = MagicMock(spec_set=AssetLookUpAPI)
        self.lookup.time_series = MagicMock(spec_set=TimeSeriesLookUpAPI)
        self.lookup.security_categories = MagicMock(spec_set=SecurityCategoriesLookUpAPI)
        self.lookup.location_filters = MagicMock(spec_set=LocationFiltersLookUpAPI)
        self.lookup.extraction_pipelines = MagicMock(spec_set=ExtractionPipelineLookUpAPI)

        self.robotics = MagicMock()
        self.robotics.robots = MagicMock(spec=RoboticsAPI)
        self.robotics.data_postprocessing = MagicMock(spec_set=DataPostProcessingAPI)
        self.robotics.locations = MagicMock(spec_set=RoboticsLocationsAPI)
        self.robotics.frames = MagicMock(spec_set=FramesAPI)
        self.robotics.maps = MagicMock(spec_set=MapsAPI)
        self.robotics.capabilities = MagicMock(spec_set=CapabilitiesAPI)

        self.verify = MagicMock(spec_set=VerifyAPI)


@contextmanager
def monkeypatch_toolkit_client() -> Iterator[ToolkitClientMock]:
    toolkit_client_mock = ToolkitClientMock()
    ToolkitClient.__new__ = lambda *args, **kwargs: toolkit_client_mock  # type: ignore[method-assign]
    yield toolkit_client_mock
    ToolkitClient.__new__ = lambda cls, *args, **kwargs: object.__new__(cls)  # type: ignore[method-assign]
