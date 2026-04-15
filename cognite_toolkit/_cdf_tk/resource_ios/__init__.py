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

from ._base_ios import DataCRUD, Loader, ResourceContainerIO, ResourceIO
from ._data_cruds import DatapointsCRUD, FileCRUD, RawFileCRUD
from ._resource_ios import (
    AgentIO,
    AssetIO,
    CogniteFileCRUD,
    ContainerCRUD,
    DataModelIO,
    DatapointSubscriptionIO,
    DataProductIO,
    DataProductVersionIO,
    DataSetsIO,
    EdgeCRUD,
    EventIO,
    ExtractionPipelineConfigIO,
    ExtractionPipelineIO,
    FileMetadataCRUD,
    FunctionIO,
    FunctionScheduleIO,
    GraphQLCRUD,
    GroupAllScopedCRUD,
    GroupIO,
    GroupResourceScopedCRUD,
    HostedExtractorDestinationIO,
    HostedExtractorJobIO,
    HostedExtractorMappingIO,
    HostedExtractorSourceIO,
    InFieldCDMLocationConfigIO,
    InFieldLocationConfigIO,
    InfieldV1IO,
    LabelIO,
    LocationFilterIO,
    NodeCRUD,
    RawDatabaseCRUD,
    RawTableCRUD,
    RelationshipIO,
    ResourceViewMappingIO,
    RobotCapabilityIO,
    RoboticFrameIO,
    RoboticLocationIO,
    RoboticMapIO,
    RoboticsDataPostProcessingIO,
    RuleSetIO,
    RuleSetVersionIO,
    SearchConfigIO,
    SecurityCategoryIO,
    SequenceIO,
    SequenceRowIO,
    SignalSinkIO,
    SignalSubscriptionIO,
    SimulatorModelIO,
    SimulatorModelRevisionIO,
    SimulatorRoutineIO,
    SimulatorRoutineRevisionIO,
    SpaceCRUD,
    StreamIO,
    StreamlitIO,
    ThreeDModelCRUD,
    TimeSeriesCRUD,
    TransformationIO,
    TransformationNotificationIO,
    TransformationScheduleIO,
    ViewIO,
    WorkflowIO,
    WorkflowTriggerIO,
    WorkflowVersionIO,
)
from ._worker import ResourceWorker

_EXCLUDED_CRUDS: set[type[ResourceIO]] = set()
if not FeatureFlag.is_enabled(Flags.GRAPHQL):
    _EXCLUDED_CRUDS.add(GraphQLCRUD)
if not FeatureFlag.is_enabled(Flags.INFIELD):
    _EXCLUDED_CRUDS.add(InfieldV1IO)
    _EXCLUDED_CRUDS.add(InFieldLocationConfigIO)
    _EXCLUDED_CRUDS.add(InFieldCDMLocationConfigIO)
if not FeatureFlag.is_enabled(Flags.MIGRATE):
    _EXCLUDED_CRUDS.add(ResourceViewMappingIO)
if not FeatureFlag.is_enabled(Flags.SIGNALS):
    _EXCLUDED_CRUDS.add(SignalSinkIO)
    _EXCLUDED_CRUDS.add(SignalSubscriptionIO)
if not FeatureFlag.is_enabled(Flags.DATA_PRODUCTS):
    _EXCLUDED_CRUDS.add(DataProductIO)
    _EXCLUDED_CRUDS.add(DataProductVersionIO)
    _EXCLUDED_CRUDS.add(RuleSetIO)
    _EXCLUDED_CRUDS.add(RuleSetVersionIO)

CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA: defaultdict[str, list[type[Loader]]] = defaultdict(list)
CRUDS_BY_FOLDER_NAME: defaultdict[str, list[type[Loader]]] = defaultdict(list)
for _loader in itertools.chain(
    ResourceIO.__subclasses__(),
    ResourceContainerIO.__subclasses__(),
    DataCRUD.__subclasses__(),
    GroupIO.__subclasses__(),
):
    if _loader in [ResourceIO, ResourceContainerIO, DataCRUD, GroupIO]:
        # Skipping base classes
        continue
    # MyPy bug: https://github.com/python/mypy/issues/4717
    CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA[_loader.folder_name].append(_loader)  # type: ignore[attr-defined, arg-type]

    if _loader not in _EXCLUDED_CRUDS:
        CRUDS_BY_FOLDER_NAME[_loader.folder_name].append(_loader)  # type: ignore[attr-defined, arg-type]
del _loader  # cleanup module namespace


# For backwards compatibility
CRUDS_BY_FOLDER_NAME["data_models"] = CRUDS_BY_FOLDER_NAME["data_modeling"]  # Todo: Remove in v1.0
CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA["data_models"] = CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA["data_modeling"]
RESOURCE_CRUD_BY_FOLDER_NAME = {
    folder_name: cruds
    for folder_name, loaders in CRUDS_BY_FOLDER_NAME.items()
    if (cruds := [crud for crud in loaders if issubclass(crud, ResourceIO)])
}

RESOURCE_CRUD_BY_FOLDER_NAME_BY_KIND: dict[str, dict[str, type[ResourceIO]]] = {
    folder_name: {crud.kind: crud for crud in cruds if issubclass(crud, ResourceIO)}
    for folder_name, cruds in RESOURCE_CRUD_BY_FOLDER_NAME.items()
}

CRUD_LIST = list(itertools.chain.from_iterable(CRUDS_BY_FOLDER_NAME.values()))
RESOURCE_CRUD_LIST = [loader for loader in CRUD_LIST if issubclass(loader, ResourceIO)]
RESOURCE_CRUD_CONTAINER_LIST = [loader for loader in CRUD_LIST if issubclass(loader, ResourceContainerIO)]
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
    "data_products",
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
    "rulesets",
    "signals",
    "simulators",
    "streams",
    "streamlit",
    "workflows",
]


def get_crud(resource_dir: str, kind: str) -> type[Loader]:
    for loader in CRUDS_BY_FOLDER_NAME[resource_dir]:
        if loader.kind == kind:
            return loader
    # Fall back to alpha-inclusive registry (e.g. for deserializing built resources
    # when a CRUD is excluded by feature flags or test patching).
    for loader in CRUDS_BY_FOLDER_NAME_INCLUDE_ALPHA[resource_dir]:
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
    "AgentIO",
    "AssetIO",
    "CogniteFileCRUD",
    "ContainerCRUD",
    "DataCRUD",
    "DataModelIO",
    "DataProductIO",
    "DataProductVersionIO",
    "DataSetsIO",
    "DatapointSubscriptionIO",
    "DatapointsCRUD",
    "EdgeCRUD",
    "EventIO",
    "ExtractionPipelineConfigIO",
    "ExtractionPipelineIO",
    "FileCRUD",
    "FileMetadataCRUD",
    "FunctionIO",
    "FunctionScheduleIO",
    "GroupAllScopedCRUD",
    "GroupIO",
    "GroupResourceScopedCRUD",
    "HostedExtractorDestinationIO",
    "HostedExtractorJobIO",
    "HostedExtractorMappingIO",
    "HostedExtractorSourceIO",
    "InFieldCDMLocationConfigIO",
    "InFieldLocationConfigIO",
    "LabelIO",
    "LocationFilterIO",
    "NodeCRUD",
    "RawDatabaseCRUD",
    "RawFileCRUD",
    "RawTableCRUD",
    "RelationshipIO",
    "ResourceContainerIO",
    "ResourceIO",
    "ResourceTypes",
    "ResourceWorker",
    "RobotCapabilityIO",
    "RoboticFrameIO",
    "RoboticLocationIO",
    "RoboticMapIO",
    "RoboticsDataPostProcessingIO",
    "RuleSetIO",
    "RuleSetVersionIO",
    "SearchConfigIO",
    "SecurityCategoryIO",
    "SequenceIO",
    "SequenceRowIO",
    "SignalSinkIO",
    "SignalSubscriptionIO",
    "SimulatorModelIO",
    "SimulatorModelRevisionIO",
    "SimulatorRoutineIO",
    "SimulatorRoutineRevisionIO",
    "SpaceCRUD",
    "StreamIO",
    "StreamlitIO",
    "ThreeDModelCRUD",
    "TimeSeriesCRUD",
    "TransformationIO",
    "TransformationNotificationIO",
    "TransformationScheduleIO",
    "ViewIO",
    "WorkflowIO",
    "WorkflowTriggerIO",
    "WorkflowVersionIO",
    "get_crud",
]
