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
from typing import Literal, TypeAlias

from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags

from ._base_loaders import DataLoader, Loader, ResourceContainerCRUD, ResourceCRUD
from ._data_loaders import DatapointsLoader, FileLoader, RawFileLoader
from ._resource_loaders import (
    AgentCRUD,
    AssetCRUD,
    CogniteFileLoader,
    ContainerLoader,
    DataModelCRUD,
    DatapointSubscriptionCRUD,
    DataSetsCRUD,
    EdgeLoader,
    EventCRUD,
    ExtractionPipelineConfigCRUD,
    ExtractionPipelineCRUD,
    FileMetadataLoader,
    FunctionCRUD,
    FunctionScheduleCRUD,
    GraphQLLoader,
    GroupAllScopedLoader,
    GroupCRUD,
    GroupResourceScopedLoader,
    HostedExtractorDestinationCRUD,
    HostedExtractorJobCRUD,
    HostedExtractorMappingCRUD,
    HostedExtractorSourceCRUD,
    InfieldV1CRUD,
    LabelCRUD,
    LocationFilterCRUD,
    NodeLoader,
    RawDatabaseLoader,
    RawTableLoader,
    RelationshipCRUD,
    RobotCapabilityCRUD,
    RoboticFrameCRUD,
    RoboticLocationCRUD,
    RoboticMapCRUD,
    RoboticsDataPostProcessingCRUD,
    SearchConfigCRUD,
    SecurityCategoryCRUD,
    SequenceCRUD,
    SequenceRowCRUD,
    SpaceLoader,
    StreamlitCRUD,
    ThreeDModelLoader,
    TimeSeriesLoader,
    TransformationCRUD,
    TransformationNotificationCRUD,
    TransformationScheduleCRUD,
    ViewCRUD,
    ViewSourceCRUD,
    WorkflowCRUD,
    WorkflowTriggerCRUD,
    WorkflowVersionCRUD,
)
from ._worker import ResourceWorker

_EXCLUDED_LOADERS: set[type[ResourceCRUD]] = set()
if not FeatureFlag.is_enabled(Flags.GRAPHQL):
    _EXCLUDED_LOADERS.add(GraphQLLoader)
if not FeatureFlag.is_enabled(Flags.AGENTS):
    _EXCLUDED_LOADERS.add(AgentCRUD)
if not FeatureFlag.is_enabled(Flags.INFIELD):
    _EXCLUDED_LOADERS.add(InfieldV1CRUD)
if not FeatureFlag.is_enabled(Flags.MIGRATE):
    _EXCLUDED_LOADERS.add(ViewSourceCRUD)
if not FeatureFlag.is_enabled(Flags.SEARCH_CONFIG):
    _EXCLUDED_LOADERS.add(SearchConfigCRUD)


LOADER_BY_FOLDER_NAME: dict[str, list[type[Loader]]] = {}
for _loader in itertools.chain(
    ResourceCRUD.__subclasses__(),
    ResourceContainerCRUD.__subclasses__(),
    DataLoader.__subclasses__(),
    GroupCRUD.__subclasses__(),
):
    if _loader in [ResourceCRUD, ResourceContainerCRUD, DataLoader, GroupCRUD] or _loader in _EXCLUDED_LOADERS:
        # Skipping base classes
        continue
    if _loader.folder_name not in LOADER_BY_FOLDER_NAME:  # type: ignore[attr-defined]
        LOADER_BY_FOLDER_NAME[_loader.folder_name] = []  # type: ignore[attr-defined]
    # MyPy bug: https://github.com/python/mypy/issues/4717
    LOADER_BY_FOLDER_NAME[_loader.folder_name].append(_loader)  # type: ignore[type-abstract, attr-defined, arg-type]
del _loader  # cleanup module namespace

LOADER_LIST = list(itertools.chain(*LOADER_BY_FOLDER_NAME.values()))
RESOURCE_LOADER_LIST = [loader for loader in LOADER_LIST if issubclass(loader, ResourceCRUD)]
RESOURCE_CONTAINER_LOADER_LIST = [loader for loader in LOADER_LIST if issubclass(loader, ResourceContainerCRUD)]
RESOURCE_DATA_LOADER_LIST = [loader for loader in LOADER_LIST if issubclass(loader, DataLoader)]
KINDS_BY_FOLDER_NAME: dict[str, set[str]] = {}
for loader in LOADER_LIST:
    if loader.folder_name not in KINDS_BY_FOLDER_NAME:
        KINDS_BY_FOLDER_NAME[loader.folder_name] = set()
    KINDS_BY_FOLDER_NAME[loader.folder_name].add(loader.kind)
del loader  # cleanup module namespace

ResourceTypes: TypeAlias = Literal[  # type: ignore[no-redef, misc]
    "3dmodels",
    "agents",
    "auth",
    "cdf_applications",
    "classic",
    "data_models",
    "data_sets",
    "hosted_extractors",
    "locations",
    "migration",
    "transformations",
    "files",
    "timeseries",
    "extraction_pipelines",
    "functions",
    "raw",
    "robotics",
    "streamlit",
    "workflows",
]


def get_loader(resource_dir: str, kind: str) -> type[Loader]:
    for loader in LOADER_BY_FOLDER_NAME[resource_dir]:
        if loader.kind == kind:
            return loader
    raise ValueError(f"Loader not found for {resource_dir} and {kind}")


__all__ = [
    "KINDS_BY_FOLDER_NAME",
    "LOADER_BY_FOLDER_NAME",
    "LOADER_LIST",
    "RESOURCE_CONTAINER_LOADER_LIST",
    "RESOURCE_DATA_LOADER_LIST",
    "RESOURCE_LOADER_LIST",
    "AssetCRUD",
    "CogniteFileLoader",
    "ContainerLoader",
    "DataLoader",
    "DataModelCRUD",
    "DataSetsCRUD",
    "DatapointSubscriptionCRUD",
    "DatapointsLoader",
    "EdgeLoader",
    "EventCRUD",
    "ExtractionPipelineCRUD",
    "ExtractionPipelineConfigCRUD",
    "FileLoader",
    "FileMetadataLoader",
    "FunctionCRUD",
    "FunctionScheduleCRUD",
    "GroupAllScopedLoader",
    "GroupCRUD",
    "GroupResourceScopedLoader",
    "HostedExtractorDestinationCRUD",
    "HostedExtractorJobCRUD",
    "HostedExtractorMappingCRUD",
    "HostedExtractorSourceCRUD",
    "LabelCRUD",
    "LocationFilterCRUD",
    "NodeLoader",
    "RawDatabaseLoader",
    "RawFileLoader",
    "RawTableLoader",
    "RelationshipCRUD",
    "ResourceCRUD",
    "ResourceContainerCRUD",
    "ResourceTypes",
    "ResourceWorker",
    "RobotCapabilityCRUD",
    "RoboticFrameCRUD",
    "RoboticLocationCRUD",
    "RoboticMapCRUD",
    "RoboticsDataPostProcessingCRUD",
    "SearchConfigCRUD",
    "SecurityCategoryCRUD",
    "SequenceCRUD",
    "SequenceRowCRUD",
    "SpaceLoader",
    "StreamlitCRUD",
    "ThreeDModelLoader",
    "TimeSeriesLoader",
    "TransformationCRUD",
    "TransformationNotificationCRUD",
    "TransformationScheduleCRUD",
    "ViewCRUD",
    "WorkflowCRUD",
    "WorkflowTriggerCRUD",
    "WorkflowVersionCRUD",
    "get_loader",
]
