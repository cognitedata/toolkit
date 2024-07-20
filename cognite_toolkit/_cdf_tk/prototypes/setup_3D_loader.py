import sys

import cognite_toolkit._cdf_tk.loaders as loaders
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    LOADER_LIST,
    RESOURCE_LOADER_LIST,
)
from cognite_toolkit._cdf_tk.prototypes.resource_loaders import ThreeDModelLoader

from . import create_resource_types

if sys.version_info >= (3, 10):
    pass
else:
    pass


_HAS_SETUP = False


def setup_model_3d_loader() -> None:
    """Set up the asset loader to be used by the Cognite Toolkit."""
    global _HAS_SETUP
    if _HAS_SETUP:
        return
    LOADER_BY_FOLDER_NAME["3dmodels"] = [ThreeDModelLoader]
    LOADER_LIST.append(ThreeDModelLoader)
    RESOURCE_LOADER_LIST.append(ThreeDModelLoader)
    _HAS_SETUP = True

    resource_types_literal = create_resource_types.create_resource_types()
    setattr(loaders, "ResourceTypes", resource_types_literal)
