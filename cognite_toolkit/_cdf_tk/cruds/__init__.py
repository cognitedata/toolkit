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
from collections import defaultdict
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
    InFieldCDMLocationConfigCRUD,
    InFieldLocationConfigCRUD,
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
    SimulatorModelCRUD,
    SpaceCRUD,
    StreamCRUD,
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
if not FeatureFlag.is_enabled(Flags.INFIELD):
    _EXCLUDED_CRUDS.add(InfieldV1CRUD)
    _EXCLUDED_CRUDS.add(InFieldLocationConfigCRUD)
    _EXCLUDED_CRUDS.add(InFieldCDMLocationConfigCRUD)
if not FeatureFlag.is_enabled(Flags.MIGRATE):
    _EXCLUDED_CRUDS.add(ResourceViewMappingCRUD)
if not FeatureFlag.is_enabled(Flags.STREAMS):
    _EXCLUDED_CRUDS.add(StreamCRUD)
if not FeatureFlag.is_enabled(Flags.SIMULATORS):
    _EXCLUDED_CRUDS.add(SimulatorModelCRUD)

CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA: defaultdict[str, list[type[Loader]]] = defaultdict(list)
CRUDS_BY_FOLDER_NAME: defaultdict[str, list[type[Loader]]] = defaultdict(list)
for _loader in itertools.chain(
    ResourceCRUD.__subclasses__(),
    ResourceContainerCRUD.__subclasses__(),
    DataCRUD.__subclasses__(),
    GroupCRUD.__subclasses__(),
):
    if _loader in [ResourceCRUD, ResourceContainerCRUD, DataCRUD, GroupCRUD]:
        # Skipping base classes
        continue
    # MyPy bug: https://github.com/python/mypy/issues/4717
    CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA[_loader.folder_name].append(_loader)  # type: ignore[arg-type, attr-defined]

    if _loader not in _EXCLUDED_CRUDS:
        CRUDS_BY_FOLDER_NAME[_loader.folder_name].append(_loader)  # type: ignore[arg-type, attr-defined]
del _loader  # cleanup module namespace


# For backwards compatibility
CRUDS_BY_FOLDER_NAME["data_models"] = CRUDS_BY_FOLDER_NAME["data_modeling"]  # Todo: Remove in v1.0
CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA["data_models"] = CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA["data_modeling"]
RESOURCE_CRUD_BY_FOLDER_NAME = {
    folder_name: cruds
    for folder_name, loaders in CRUDS_BY_FOLDER_NAME.items()
    if (cruds := [crud for crud in loaders if issubclass(crud, ResourceCRUD)])
}

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
    "data_modeling",
    "data_models",  # Todo: Remove in v1.0
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
    "simulators",
    "streams",
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
    "RESOURCE_CRUD_BY_FOLDER_NAME",
    "RESOURCE_CRUD_CONTAINER_LIST",
    "RESOURCE_CRUD_LIST",
    "RESOURCE_DATA_CRUD_LIST",
    "_EXCLUDED_CRUDS",
    "AgentCRUD",
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
    "InFieldCDMLocationConfigCRUD",
    "InFieldLocationConfigCRUD",
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
    "SimulatorModelCRUD",
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
