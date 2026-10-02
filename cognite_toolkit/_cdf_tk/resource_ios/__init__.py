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

from ._agent import AgentIO
from ._app import AppIO, AppVersionIO
from ._auth import GroupAllScopedIO, GroupIO, SecurityCategoryIO
from ._base_ios import ResourceBuildIO, ResourceContainerIO, ResourceIO, ResourceType
from ._classic import AssetIO, EventIO, SequenceIO, SequenceRowIO
from ._configuration import SearchConfigIO
from ._data_organization import DataSetsIO, LabelIO
from ._data_product import DataProductIO
from ._data_product_version import DataProductVersionIO
from ._datamodel import (
    ContainerIO,
    DataModelIO,
    EdgeIO,
    GraphQLIO,
    NodeIO,
    SpaceIO,
    ViewIO,
)
from ._externaldata import ExternalDataSourceIO
from ._extraction_pipeline import ExtractionPipelineConfigIO, ExtractionPipelineIO
from ._fieldops import InFieldCDMLocationConfigIO, InFieldLocationConfigIO, InfieldV1IO
from ._file import CogniteFileIO, FileMetadataIO
from ._function import FunctionIO, FunctionScheduleIO
from ._group_scoped import GroupResourceScopedIO
from ._hosted_extractors import (
    HostedExtractorDestinationIO,
    HostedExtractorJobIO,
    HostedExtractorMappingIO,
    HostedExtractorSourceIO,
)
from ._industrial_tool import StreamlitIO
from ._integrations import IntegrationConfigsIO, IntegrationsIO
from ._location import LocationFilterIO
from ._migration import ResourceViewMappingIO
from ._raw import RawDatabaseIO, RawTableIO
from ._relationship import RelationshipIO
from ._rulesets import RuleSetIO, RuleSetVersionIO
from ._sap_writeback import SAPEndpointIO, SAPInstanceIO, SchemaMappingIO
from ._signal_sink import SignalSinkIO
from ._signal_subscription import SignalSubscriptionIO
from ._simulators import (
    SimulatorModelIO,
    SimulatorModelRevisionIO,
    SimulatorRoutineIO,
    SimulatorRoutineRevisionIO,
)
from ._skill import SkillIO
from ._streams import StreamIO
from ._three_d_model import ThreeDModelIO
from ._timeseries import DatapointSubscriptionIO, TimeSeriesIO
from ._transformation import (
    TransformationIO,
    TransformationNotificationIO,
    TransformationScheduleIO,
)
from ._workflow import WorkflowIO, WorkflowTriggerIO, WorkflowVersionIO

_EXCLUDED_CRUDS: set[type[ResourceIO]] = set()
if not FeatureFlag.is_enabled(Flags.GRAPHQL):
    _EXCLUDED_CRUDS.add(GraphQLIO)
if not FeatureFlag.is_enabled(Flags.INFIELD):
    _EXCLUDED_CRUDS.add(InfieldV1IO)
    _EXCLUDED_CRUDS.add(InFieldLocationConfigIO)
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
if not FeatureFlag.is_enabled(Flags.CUSTOM_APPS):
    _EXCLUDED_CRUDS.add(AppIO)
    _EXCLUDED_CRUDS.add(AppVersionIO)
if not FeatureFlag.is_enabled(Flags.AGENT_SKILLS):
    _EXCLUDED_CRUDS.add(SkillIO)
if not FeatureFlag.is_enabled(Flags.EXTERNAL_DATA_SOURCES):
    _EXCLUDED_CRUDS.add(ExternalDataSourceIO)
if not FeatureFlag.is_enabled(Flags.SAP_WRITEBACK):
    _EXCLUDED_CRUDS.add(SAPInstanceIO)
    _EXCLUDED_CRUDS.add(SAPEndpointIO)
    _EXCLUDED_CRUDS.add(SchemaMappingIO)
if not FeatureFlag.is_enabled(Flags.INTEGRATIONS):
    _EXCLUDED_CRUDS.add(IntegrationsIO)
    _EXCLUDED_CRUDS.add(IntegrationConfigsIO)

RESOURCE_BUILD_IO_BY_FOLDER_NAME_INCLUDE_ALPHA: defaultdict[str, list[type[ResourceBuildIO]]] = defaultdict(list)
RESOURCE_IO_BY_FOLDER_NAME: defaultdict[str, list[type[ResourceIO]]] = defaultdict(list)
RESOURCE_BUILD_IO_BY_FOLDER_NAME: defaultdict[str, list[type[ResourceBuildIO]]] = defaultdict(list)
for _io_cls in itertools.chain(
    ResourceIO.__subclasses__(),
    ResourceContainerIO.__subclasses__(),
    GroupIO.__subclasses__(),
    ResourceBuildIO.__subclasses__(),
):
    if _io_cls in [ResourceIO, ResourceContainerIO, GroupIO, ResourceBuildIO]:
        # Skipping base classes
        continue
    # MyPy bug: https://github.com/python/mypy/issues/4717
    RESOURCE_BUILD_IO_BY_FOLDER_NAME_INCLUDE_ALPHA[_io_cls.folder_name].append(_io_cls)  # type: ignore[attr-defined, arg-type]

    if _io_cls not in _EXCLUDED_CRUDS:
        if issubclass(_io_cls, ResourceIO):
            RESOURCE_IO_BY_FOLDER_NAME[_io_cls.folder_name].append(_io_cls)
        RESOURCE_BUILD_IO_BY_FOLDER_NAME[_io_cls.folder_name].append(_io_cls)  # type: ignore[attr-defined, arg-type]
del _io_cls  # cleanup module namespace


# For backwards compatibility
RESOURCE_IO_BY_FOLDER_NAME["data_models"] = RESOURCE_IO_BY_FOLDER_NAME["data_modeling"]  # Todo: Remove in v1.0
RESOURCE_BUILD_IO_BY_FOLDER_NAME_INCLUDE_ALPHA["data_models"] = RESOURCE_BUILD_IO_BY_FOLDER_NAME_INCLUDE_ALPHA[
    "data_modeling"
]

RESOURCE_BUILD_IO_BY_TYPE = {
    ResourceType(resource_folder=folder_name, kind=crud.kind): crud
    for folder_name, cruds in RESOURCE_BUILD_IO_BY_FOLDER_NAME.items()
    for crud in cruds
}
RESOURCE_IO_BY_TYPE = {
    ResourceType(resource_folder=folder_name, kind=crud.kind): crud
    for folder_name, cruds in RESOURCE_IO_BY_FOLDER_NAME.items()
    for crud in cruds
}

RESOURCE_BUILD_IO_LIST: list[type[ResourceBuildIO]] = list(
    itertools.chain.from_iterable(RESOURCE_BUILD_IO_BY_FOLDER_NAME.values())
)
RESOURCE_IO_LIST = [io_cls for io_cls in RESOURCE_BUILD_IO_LIST if issubclass(io_cls, ResourceIO)]


ResourceTypes: TypeAlias = Literal[
    "3dmodels",
    "agents",
    "skills",
    "apps",
    "auth",
    "cdf_applications",
    "classic",
    "data_modeling",
    "data_models",  # Todo: Remove in v1.0
    "data_products",
    "data_sets",
    "hosted_extractors",
    "integrations",
    "locations",
    "migration",
    "transformations",
    "files",
    "timeseries",
    "extraction_pipelines",
    "functions",
    "raw",
    "rulesets",
    "SAPwritebacks",
    "signals",
    "simulators",
    "streams",
    "streamlit",
    "workflows",
]


def get_resource_build_io(resource_dir: str, kind: str) -> type[ResourceBuildIO]:
    if io_cls := RESOURCE_BUILD_IO_BY_TYPE.get(ResourceType(resource_folder=resource_dir, kind=kind)):
        return io_cls
    # Fall back to alpha-inclusive registry (e.g. for deserializing built resources
    # when a CRUD is excluded by feature flags or test patching).
    for loader in RESOURCE_BUILD_IO_BY_FOLDER_NAME_INCLUDE_ALPHA[resource_dir]:
        if loader.kind == kind:
            return loader
    raise ValueError(f"Loader not found for {resource_dir} and {kind}")


__all__ = [
    "RESOURCE_BUILD_IO_LIST",
    "RESOURCE_IO_BY_FOLDER_NAME",
    "RESOURCE_IO_LIST",
    "AgentIO",
    "AppIO",
    "AppVersionIO",
    "AssetIO",
    "CogniteFileIO",
    "ContainerIO",
    "DataModelIO",
    "DataProductIO",
    "DataProductVersionIO",
    "DataSetsIO",
    "DatapointSubscriptionIO",
    "EdgeIO",
    "EventIO",
    "ExternalDataSourceIO",
    "ExtractionPipelineConfigIO",
    "ExtractionPipelineIO",
    "FileMetadataIO",
    "FunctionIO",
    "FunctionScheduleIO",
    "GroupAllScopedIO",
    "GroupIO",
    "GroupResourceScopedIO",
    "HostedExtractorDestinationIO",
    "HostedExtractorJobIO",
    "HostedExtractorMappingIO",
    "HostedExtractorSourceIO",
    "InFieldCDMLocationConfigIO",
    "InFieldLocationConfigIO",
    "IntegrationConfigsIO",
    "IntegrationsIO",
    "LabelIO",
    "LocationFilterIO",
    "NodeIO",
    "RawDatabaseIO",
    "RawTableIO",
    "RelationshipIO",
    "ResourceBuildIO",
    "ResourceContainerIO",
    "ResourceIO",
    "ResourceType",
    "ResourceTypes",
    "RuleSetIO",
    "RuleSetVersionIO",
    "SAPEndpointIO",
    "SAPInstanceIO",
    "SchemaMappingIO",
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
    "SkillIO",
    "SpaceIO",
    "StreamIO",
    "StreamlitIO",
    "ThreeDModelIO",
    "TimeSeriesIO",
    "TransformationIO",
    "TransformationNotificationIO",
    "TransformationScheduleIO",
    "ViewIO",
    "WorkflowIO",
    "WorkflowTriggerIO",
    "WorkflowVersionIO",
    "get_resource_build_io",
]
