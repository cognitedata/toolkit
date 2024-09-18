# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import itertools
import sys
from typing import Literal

from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags

from ._base_loaders import DataLoader, Loader, ResourceContainerLoader, ResourceLoader
from ._data_loaders import DatapointsLoader, FileLoader, RawFileLoader
from ._resource_loaders import (
    ContainerLoader,
    DataModelLoader,
    DataSetsLoader,
    ExtractionPipelineConfigLoader,
    ExtractionPipelineLoader,
    FileMetadataLoader,
    FunctionLoader,
    FunctionScheduleLoader,
    GraphQLLoader,
    GroupAllScopedLoader,
    GroupLoader,
    GroupResourceScopedLoader,
    HostedExtractorSourceLoader,
    LabelLoader,
    LocationFilterLoader,
    NodeLoader,
    RawDatabaseLoader,
    RawTableLoader,
    SequenceLoader,
    SpaceLoader,
    TimeSeriesLoader,
    TransformationLoader,
    TransformationScheduleLoader,
    ViewLoader,
    WorkflowLoader,
    WorkflowVersionLoader,
)
from .data_classes import DeployResult, DeployResults

if sys.version_info >= (3, 10):
    from typing import TypeAlias
else:
    from typing_extensions import TypeAlias

_EXCLUDED_LOADERS: set[type[ResourceLoader]] = set()
if not FeatureFlag.is_enabled(Flags.GRAPHQL):
    _EXCLUDED_LOADERS.add(GraphQLLoader)
if not FeatureFlag.is_enabled(Flags.HOSTED_EXTRACTORS):
    _EXCLUDED_LOADERS.add(HostedExtractorSourceLoader)

LOADER_BY_FOLDER_NAME: dict[str, list[type[Loader]]] = {}
for _loader in itertools.chain(
    ResourceLoader.__subclasses__(),
    ResourceContainerLoader.__subclasses__(),
    DataLoader.__subclasses__(),
    GroupLoader.__subclasses__(),
):
    if _loader in [ResourceLoader, ResourceContainerLoader, DataLoader, GroupLoader] or _loader in _EXCLUDED_LOADERS:
        # Skipping base classes
        continue
    if _loader.folder_name not in LOADER_BY_FOLDER_NAME:  # type: ignore[attr-defined]
        LOADER_BY_FOLDER_NAME[_loader.folder_name] = []  # type: ignore[attr-defined]
    # MyPy bug: https://github.com/python/mypy/issues/4717
    LOADER_BY_FOLDER_NAME[_loader.folder_name].append(_loader)  # type: ignore[type-abstract, attr-defined, arg-type]
del _loader  # cleanup module namespace

LOADER_LIST = list(itertools.chain(*LOADER_BY_FOLDER_NAME.values()))
RESOURCE_LOADER_LIST = [loader for loader in LOADER_LIST if issubclass(loader, ResourceLoader)]
RESOURCE_CONTAINER_LOADER_LIST = [loader for loader in LOADER_LIST if issubclass(loader, ResourceContainerLoader)]

ResourceTypes: TypeAlias = Literal[
    "3dmodels",
    "auth",
    "classic",
    "data_models",
    "data_sets",
    "hosted_extractors",
    "locations",
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


def get_loader(resource_dir: str, kind: str) -> type[Loader]:
    for loader in LOADER_BY_FOLDER_NAME[resource_dir]:
        if loader.kind == kind:
            return loader
    raise ValueError(f"Loader not found for {resource_dir} and {kind}")


__all__ = [
    "GroupLoader",
    "GroupAllScopedLoader",
    "GroupResourceScopedLoader",
    "NodeLoader",
    "SequenceLoader",
    "DataModelLoader",
    "DataSetsLoader",
    "SpaceLoader",
    "ContainerLoader",
    "FileMetadataLoader",
    "FileLoader",
    "FunctionLoader",
    "FunctionScheduleLoader",
    "TimeSeriesLoader",
    "RawDatabaseLoader",
    "RawTableLoader",
    "RawFileLoader",
    "TransformationLoader",
    "TransformationScheduleLoader",
    "ExtractionPipelineLoader",
    "ExtractionPipelineConfigLoader",
    "LabelLoader",
    "LocationFilterLoader",
    "ViewLoader",
    "DatapointsLoader",
    "ResourceLoader",
    "ResourceContainerLoader",
    "DataLoader",
    "DeployResult",
    "DeployResults",
    "ResourceTypes",
    "WorkflowLoader",
    "WorkflowVersionLoader",
    "HostedExtractorSourceLoader",
    "get_loader",
    "LOADER_BY_FOLDER_NAME",
    "LOADER_LIST",
    "RESOURCE_LOADER_LIST",
    "RESOURCE_CONTAINER_LOADER_LIST",
]
