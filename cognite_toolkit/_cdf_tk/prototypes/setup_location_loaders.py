import cognite_toolkit._cdf_tk.loaders as loaders
from cognite_toolkit._cdf_tk.loaders import (
    LOADER_BY_FOLDER_NAME,
    LOADER_LIST,
    RESOURCE_LOADER_LIST,
)
from cognite_toolkit._cdf_tk.prototypes import create_resource_types
from cognite_toolkit._cdf_tk.prototypes.location_loaders import LocationFilterLoader

_HAS_SETUP = False


def setup_location_loaders() -> None:
    """Set up the asset loader to be used by the Cognite Toolkit."""
    global _HAS_SETUP
    if _HAS_SETUP:
        return
    LOADER_BY_FOLDER_NAME["locations"] = [
        LocationFilterLoader,
    ]
    for loader in LOADER_BY_FOLDER_NAME["locations"]:
        LOADER_LIST.append(loader)
        if issubclass(loader, loaders.ResourceLoader):
            RESOURCE_LOADER_LIST.append(loader)

    _HAS_SETUP = True
    resource_types_literal = create_resource_types.create_resource_types()
    setattr(loaders, "ResourceTypes", resource_types_literal)
