import json
import re
import sys
from collections.abc import Hashable, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

from cognite.client.data_classes import FileMetadataWriteList, TimeSeriesWriteList

import cognite_toolkit._cdf_tk.loaders as loaders
from cognite_toolkit._cdf_tk._parameters import ANY_INT, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.exceptions import ToolkitFileNotFoundError
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    LOADER_LIST,
    RESOURCE_LOADER_LIST,
    FileMetadataLoader,
    ResourceLoader,
    TimeSeriesLoader,
)
from cognite_toolkit._cdf_tk.prototypes.resource_loaders import AssetLoader
from cognite_toolkit._cdf_tk.utils import CDFToolConfig, load_yaml_inject_variables

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

ResourceTypes: TypeAlias = Literal[
    "assets",
    "model3D",
    "auth",
    "data_models",
    "data_sets",
    "labels",
    "transformations",
    "files",
    "timeseries",
    "timeseries_datapoints",
    "extraction_pipelines",
    "functions",
    "raw",
    "robotics",
    "workflows",
]

_HAS_SETUP = False


def setup_model_3d_loader() -> None:
    """Set up the asset loader to be used by the Cognite Toolkit."""
    global _HAS_SETUP
    if _HAS_SETUP:
        return
    LOADER_BY_FOLDER_NAME["model3D"] = [AssetLoader]
    LOADER_LIST.append(AssetLoader)
    RESOURCE_LOADER_LIST.append(AssetLoader)
    setattr(loaders, "ResourceTypes", ResourceTypes)
    _modify_timeseries_loader()
    _modify_file_metadata_loader()
    _HAS_SETUP = True

