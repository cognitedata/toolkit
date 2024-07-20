from typing import Any, Literal


def create_resource_types() -> Any:
    from . import setup_3D_loader, setup_asset_loader, setup_robotics_loaders

    resource_types = [
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
        "workflows",
    ]
    if setup_asset_loader._HAS_SETUP:
        resource_types.append("assets")
    if setup_robotics_loaders._HAS_SETUP:
        resource_types.append("robotics")
    if setup_3D_loader._HAS_SETUP:
        resource_types.append("3dmodels")
    return Literal[tuple(resource_types)]
