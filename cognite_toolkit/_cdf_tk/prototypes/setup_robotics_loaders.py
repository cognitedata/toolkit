import sys
from typing import Literal

import cognite_toolkit._cdf_tk.loaders as loaders
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    LOADER_LIST,
    RESOURCE_LOADER_LIST,
)
from cognite_toolkit._cdf_tk.prototypes.robotics_loaders import (
    RobotCapabilityLoader,
    RoboticFrameLoader,
    RoboticLocationLoader,
    RoboticMapLoader,
    RoboticsDataPostProcessingLoader,
)

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

ResourceTypes: TypeAlias = Literal[
    "assets",
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


def setup_robotics_loaders() -> None:
    """Set up the asset loader to be used by the Cognite Toolkit."""
    global _HAS_SETUP
    if _HAS_SETUP:
        return
    LOADER_BY_FOLDER_NAME["robotics"] = [
        RobotCapabilityLoader,
        RoboticFrameLoader,
        RoboticsDataPostProcessingLoader,
        RoboticLocationLoader,
        RoboticMapLoader,
    ]
    for loader in LOADER_BY_FOLDER_NAME["robotics"]:
        LOADER_LIST.append(loader)
        if issubclass(loader, loaders.ResourceLoader):
            RESOURCE_LOADER_LIST.append(loader)

    setattr(loaders, "ResourceTypes", ResourceTypes)
    _HAS_SETUP = True
