"""Toolkit resource classes are pydantic models that represent the YAML file format the Cognite Toolkit uses.

This is means that we have three set of resource classes we use in Toolkit:

1. Toolkit resource classes (this module): Represent the YAML configuration format the Toolkit uses.
2. Write resource classes (from cognite-sdk): Represent the write/request format the Cognite resources.
3. Read resource classes (from cognite-sdk): Represent the read/response format the Cognite resources.
"""

from .asset import AssetYAML
from .base import BaseModelResource, ToolkitResource
from .dataset import DataSetYAML
from .event import EventYAML
from .filemetadata import FileMetadataYAML
from .function_schedule import FunctionScheduleYAML
from .functions import FunctionsYAML
from .groups import GroupYAML
from .labels import LabelsYAML
from .location import LocationYAML
from .raw_database_table import DatabaseYAML, TableYAML
from .search_config import SearchConfigYAML
from .securitycategories import SecurityCategoriesYAML
from .space import SpaceYAML
from .threedmodels import ThreeDModelYAML
from .timeseries import TimeSeriesYAML
from .transformation_schedule import TransformationScheduleYAML
from .transformations import TransformationYAML

__all__ = [
    "AssetYAML",
    "BaseModelResource",
    "DataSetYAML",
    "DatabaseYAML",
    "EventYAML",
    "FileMetadataYAML",
    "FunctionScheduleYAML",
    "FunctionsYAML",
    "GroupYAML",
    "LabelsYAML",
    "LocationYAML",
    "SearchConfigYAML",
    "SecurityCategoriesYAML",
    "SpaceYAML",
    "TableYAML",
    "ThreeDModelYAML",
    "TimeSeriesYAML",
    "ToolkitResource",
    "TransformationScheduleYAML",
    "TransformationYAML",
]
