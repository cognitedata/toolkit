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

from ._base_cruds import DataCRUD, Loader, ResourceContainerCRUD, ResourceCRUD
from ._data_cruds import DatapointsCRUD, FileCRUD, RawFileCRUD
from ._resource_cruds import (
    AgentCRUD,
    AssetCRUD,
    CogniteFileCRUD,
    ContainerCRUD,
    DataModelCRUD,
    DatapointSubscriptionCRUD,
    DataSetsCRUD,
    EdgeCRUD,
    EventCRUD,
    ExtractionPipelineConfigCRUD,
    ExtractionPipelineCRUD,
    FileMetadataCRUD,
    FunctionCRUD,
    FunctionScheduleCRUD,
    GraphQLCRUD,
    GroupAllScopedCRUD,
    GroupCRUD,
    GroupResourceScopedCRUD,
    HostedExtractorDestinationCRUD,
    HostedExtractorJobCRUD,
    HostedExtractorMappingCRUD,
    HostedExtractorSourceCRUD,
    InfieldV1CRUD,
    LabelCRUD,
    LocationFilterCRUD,
    NodeCRUD,
    RawDatabaseCRUD,
    RawTableCRUD,
    RelationshipCRUD,
    ResourceViewMappingCRUD,
    RobotCapabilityCRUD,
    RoboticFrameCRUD,
    RoboticLocationCRUD,
    RoboticMapCRUD,
    RoboticsDataPostProcessingCRUD,
    SearchConfigCRUD,
    SecurityCategoryCRUD,
    SequenceCRUD,
    SequenceRowCRUD,
    SpaceCRUD,
    StreamlitCRUD,
    ThreeDModelCRUD,
    TimeSeriesCRUD,
    TransformationCRUD,
    TransformationNotificationCRUD,
    TransformationScheduleCRUD,
    ViewCRUD,
    WorkflowCRUD,
    WorkflowTriggerCRUD,
    WorkflowVersionCRUD,
)
from ._worker import ResourceWorker

_EXCLUDED_CRUDS: set[type[ResourceCRUD]] = set()
if not FeatureFlag.is_enabled(Flags.GRAPHQL):
    _EXCLUDED_CRUDS.add(GraphQLCRUD)
if not FeatureFlag.is_enabled(Flags.AGENTS):
    _EXCLUDED_CRUDS.add(AgentCRUD)
if not FeatureFlag.is_enabled(Flags.INFIELD):
    _EXCLUDED_CRUDS.add(InfieldV1CRUD)
if not FeatureFlag.is_enabled(Flags.MIGRATE):
    _EXCLUDED_CRUDS.add(ResourceViewMappingCRUD)
if not FeatureFlag.is_enabled(Flags.SEARCH_CONFIG):
    _EXCLUDED_CRUDS.add(SearchConfigCRUD)


CRUDS_BY_FOLDER_NAME: dict[str, list[type[Loader]]] = {}
for _loader in itertools.chain(
    ResourceCRUD.__subclasses__(),
    ResourceContainerCRUD.__subclasses__(),
    DataCRUD.__subclasses__(),
    GroupCRUD.__subclasses__(),
):
    if _loader in [ResourceCRUD, ResourceContainerCRUD, DataCRUD, GroupCRUD] or _loader in _EXCLUDED_CRUDS:
        # Skipping base classes
        continue
    if _loader.folder_name not in CRUDS_BY_FOLDER_NAME:  # type: ignore[attr-defined]
        CRUDS_BY_FOLDER_NAME[_loader.folder_name] = []  # type: ignore[attr-defined]
    # MyPy bug: https://github.com/python/mypy/issues/4717
    CRUDS_BY_FOLDER_NAME[_loader.folder_name].append(_loader)  # type: ignore[arg-type, attr-defined]
del _loader  # cleanup module namespace

CRUD_LIST = list(itertools.chain.from_iterable(CRUDS_BY_FOLDER_NAME.values()))
RESOURCE_CRUD_LIST = [loader for loader in CRUD_LIST if issubclass(loader, ResourceCRUD)]
RESOURCE_CRUD_CONTAINER_LIST = [loader for loader in CRUD_LIST if issubclass(loader, ResourceContainerCRUD)]
RESOURCE_DATA_CRUD_LIST = [loader for loader in CRUD_LIST if issubclass(loader, DataCRUD)]
KINDS_BY_FOLDER_NAME: dict[str, set[str]] = {}
for crud in CRUD_LIST:
    if crud.folder_name not in KINDS_BY_FOLDER_NAME:
        KINDS_BY_FOLDER_NAME[crud.folder_name] = set()
    KINDS_BY_FOLDER_NAME[crud.folder_name].add(crud.kind)
del crud  # cleanup module namespace

ResourceTypes: TypeAlias = Literal[
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


def get_crud(resource_dir: str, kind: str) -> type[Loader]:
    for loader in CRUDS_BY_FOLDER_NAME[resource_dir]:
        if loader.kind == kind:
            return loader
    raise ValueError(f"Loader not found for {resource_dir} and {kind}")


__all__ = [
    "CRUDS_BY_FOLDER_NAME",
    "CRUD_LIST",
    "KINDS_BY_FOLDER_NAME",
    "RESOURCE_CRUD_CONTAINER_LIST",
    "RESOURCE_CRUD_LIST",
    "RESOURCE_DATA_CRUD_LIST",
    "AssetCRUD",
    "CogniteFileCRUD",
    "ContainerCRUD",
    "DataCRUD",
    "DataModelCRUD",
    "DataSetsCRUD",
    "DatapointSubscriptionCRUD",
    "DatapointsCRUD",
    "EdgeCRUD",
    "EventCRUD",
    "ExtractionPipelineCRUD",
    "ExtractionPipelineConfigCRUD",
    "FileCRUD",
    "FileMetadataCRUD",
    "FunctionCRUD",
    "FunctionScheduleCRUD",
    "GroupAllScopedCRUD",
    "GroupCRUD",
    "GroupResourceScopedCRUD",
    "HostedExtractorDestinationCRUD",
    "HostedExtractorJobCRUD",
    "HostedExtractorMappingCRUD",
    "HostedExtractorSourceCRUD",
    "LabelCRUD",
    "LocationFilterCRUD",
    "NodeCRUD",
    "RawDatabaseCRUD",
    "RawFileCRUD",
    "RawTableCRUD",
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
    "SpaceCRUD",
    "StreamlitCRUD",
    "ThreeDModelCRUD",
    "TimeSeriesCRUD",
    "TransformationCRUD",
    "TransformationNotificationCRUD",
    "TransformationScheduleCRUD",
    "ViewCRUD",
    "WorkflowCRUD",
    "WorkflowTriggerCRUD",
    "WorkflowVersionCRUD",
    "get_crud",
]
