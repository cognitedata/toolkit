"""Toolkit resource classes are pydantic models that represent the YAML file format the Cognite Toolkit uses.

This is means that we have three set of resource classes we use in Toolkit:

1. Toolkit resource classes (this module): Represent the YAML configuration format the Toolkit uses.
2. Write resource classes (from cognite-sdk): Represent the write/request format the Cognite resources.
3. Read resource classes (from cognite-sdk): Represent the read/response format the Cognite resources.
"""

from .base import BaseModelResource, ToolkitResource
from .dataset import DataSetYAML
from .groups import GroupYAML
from .timeseries import TimeSeriesYAML

__all__ = [
    "BaseModelResource",
    "DataSetYAML",
    "GroupYAML",
    "TimeSeriesYAML",
    "ToolkitResource",
]
