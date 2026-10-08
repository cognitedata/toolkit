from typing import Annotated, Any, Literal

from pydantic import Field, model_serializer
from pydantic.functional_validators import BeforeValidator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

from .base import BaseModelResource

# Populated after the scope and capability classes are defined. Validators run later, at call time.
_KNOWN_SCOPE_NAMES: set[str] = set()
_KNOWN_CAPABILITY_NAMES: set[str] = set()


def _lift_name(value: Any, field_name: str, alias: str) -> Any:
    """Turn a one-key YAML object, such as ``{"all": {}}``, into ``{alias: "all"}``."""
    if isinstance(value, dict) and field_name not in value and alias not in value and len(value) == 1:
        name, content = next(iter(value.items()))
        if isinstance(content, dict):
            return {alias: name, **content}
    return value


def _lift_scope_name(value: Any) -> Any:
    if isinstance(value, Scope):
        return value
    return _lift_name(value, "scope_name", "scopeName")


def _lift_scope_name_or_unknown(value: Any) -> Any:
    if isinstance(value, Scope):
        return value
    lifted = _lift_scope_name(value)
    if not isinstance(lifted, dict):
        return lifted
    name = lifted.get("scopeName", lifted.get("scope_name"))
    if not isinstance(name, str) or name in _KNOWN_SCOPE_NAMES or name == "__unknown__":
        return lifted
    return {"scopeName": "__unknown__", "unknownName": name, "rawData": value}


def _lift_capability_name(value: Any) -> Any:
    if isinstance(value, Capability):
        return value
    return _lift_name(value, "capability_name", "capabilityName")


def _lift_capability_name_or_unknown(value: Any) -> Any:
    if isinstance(value, Capability):
        return value
    lifted = _lift_capability_name(value)
    if not isinstance(lifted, dict):
        return lifted
    name = lifted.get("capabilityName", lifted.get("capability_name"))
    if not isinstance(name, str) or name in _KNOWN_CAPABILITY_NAMES or name == "__unknown__":
        return lifted
    raw = value if isinstance(value, dict) else {}
    content = next(iter(raw.values()), {}) if len(raw) == 1 else {}
    scope = content.get("scope") if isinstance(content, dict) else None
    return {
        "capabilityName": "__unknown__",
        "unknownName": name,
        "rawData": raw,
        "scope": scope if isinstance(scope, dict) else {"all": {}},
        "actions": [],
    }


class Scope(BaseModelResource):
    scope_name: str = Field(exclude=True)

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_scope_name(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        return {self.scope_name: serialized_data}


class AgentExternalIdScope(Scope):
    scope_name: Literal["agentExternalIdScope"] = Field("agentExternalIdScope", exclude=True)
    external_ids: list[str]


class AllScope(Scope):
    scope_name: Literal["all"] = Field("all", exclude=True)


class AppConfigScope(Scope):
    scope_name: Literal["appScope"] = Field("appScope", exclude=True)
    apps: list[Literal["SEARCH"]]


class AppExternalIdScope(Scope):
    scope_name: Literal["appExternalIdScope"] = Field("appExternalIdScope", exclude=True)
    external_ids: list[str]


class DataProductScope(Scope):
    scope_name: Literal["dataProductScope"] = Field("dataProductScope", exclude=True)
    external_ids: list[str]


class CurrentUserScope(Scope):
    scope_name: Literal["currentuserscope"] = Field("currentuserscope", exclude=True)


class IDScope(Scope):
    scope_name: Literal["idScope"] = Field("idScope", exclude=True)
    ids: list[str]


class IDScopeLowerCase(Scope):
    """Necessary due to lack of API standardisation on scope name: 'idScope' VS 'idscope'"""

    scope_name: Literal["idscope"] = Field("idscope", exclude=True)
    ids: list[str]


class InstancesScope(Scope):
    scope_name: Literal["instancesScope"] = Field("instancesScope", exclude=True)
    instances: list[str]


class ExtractionPipelineScope(Scope):
    scope_name: Literal["extractionPipelineScope"] = Field("extractionPipelineScope", exclude=True)
    ids: list[str]


class PostgresGatewayUsersScope(Scope):
    scope_name: Literal["usersScope"] = Field("usersScope", exclude=True)
    usernames: list[str]


class DataSetScope(Scope):
    scope_name: Literal["datasetScope"] = Field("datasetScope", exclude=True)
    ids: list[str]


class TableScope(Scope):
    scope_name: Literal["tableScope"] = Field("tableScope", exclude=True)
    dbs_to_tables: dict[str, list[str]]


class AssetRootIDScope(Scope):
    scope_name: Literal["assetRootIdScope"] = Field("assetRootIdScope", exclude=True)
    root_ids: list[str]


class ExperimentScope(Scope):
    scope_name: Literal["experimentscope"] = Field("experimentscope", exclude=True)
    experiments: list[str]


class SpaceIDScope(Scope):
    scope_name: Literal["spaceIdScope"] = Field("spaceIdScope", exclude=True)
    space_ids: list[str]


class PartitionScope(Scope):
    scope_name: Literal["partition"] = Field("partition", exclude=True)
    partition_ids: list[int]


class LegacySpaceScope(Scope):
    scope_name: Literal["spaceScope"] = Field("spaceScope", exclude=True)
    external_ids: list[str]


class LegacyDataModelScope(Scope):
    scope_name: Literal["dataModelScope"] = Field("dataModelScope", exclude=True)
    external_ids: list[str]


_LiftScopeName = BeforeValidator(_lift_scope_name)

AllScopeType = Annotated[AllScope, _LiftScopeName]
ExperimentScopeType = Annotated[ExperimentScope, _LiftScopeName]
AllOrAgentExternalIdScope = Annotated[
    AllScope | AgentExternalIdScope, Field(discriminator="scope_name"), _LiftScopeName
]
AllOrAppConfigScope = Annotated[AllScope | AppConfigScope, Field(discriminator="scope_name"), _LiftScopeName]
AllOrAppExternalIdScope = Annotated[AllScope | AppExternalIdScope, Field(discriminator="scope_name"), _LiftScopeName]
AllOrCurrentUserScope = Annotated[AllScope | CurrentUserScope, Field(discriminator="scope_name"), _LiftScopeName]
AllOrDataProductScope = Annotated[AllScope | DataProductScope, Field(discriminator="scope_name"), _LiftScopeName]
AllOrDataSetScope = Annotated[AllScope | DataSetScope, Field(discriminator="scope_name"), _LiftScopeName]
AllOrIDScope = Annotated[AllScope | IDScope, Field(discriminator="scope_name"), _LiftScopeName]
AllOrIDScopeLowerCase = Annotated[AllScope | IDScopeLowerCase, Field(discriminator="scope_name"), _LiftScopeName]
AllOrInstancesScope = Annotated[AllScope | InstancesScope, Field(discriminator="scope_name"), _LiftScopeName]
AllOrPartitionScope = Annotated[AllScope | PartitionScope, Field(discriminator="scope_name"), _LiftScopeName]
AllOrPostgresGatewayUsersScope = Annotated[
    AllScope | PostgresGatewayUsersScope, Field(discriminator="scope_name"), _LiftScopeName
]
AllOrSpaceIDScope = Annotated[AllScope | SpaceIDScope, Field(discriminator="scope_name"), _LiftScopeName]
AllOrTableScope = Annotated[AllScope | TableScope, Field(discriminator="scope_name"), _LiftScopeName]
AllIDOrDataSetScope = Annotated[AllScope | IDScope | DataSetScope, Field(discriminator="scope_name"), _LiftScopeName]
AllDataSetOrExtractionPipelineScope = Annotated[
    AllScope | DataSetScope | ExtractionPipelineScope, Field(discriminator="scope_name"), _LiftScopeName
]
AllDataSetIDOrAssetRootScope = Annotated[
    AllScope | DataSetScope | IDScopeLowerCase | AssetRootIDScope, Field(discriminator="scope_name"), _LiftScopeName
]


class Capability(BaseModelResource):
    capability_name: str = Field(exclude=True)
    scope: Scope

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_capability_name(self, handler: SerializerFunctionWrapHandler) -> dict:
        serialized_data = handler(self)
        return {self.capability_name: serialized_data}


class AgentsAcl(Capability):
    capability_name: Literal["agentsAcl"] = Field("agentsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "RUN"]]
    scope: AllOrAgentExternalIdScope


class AnalyticsAcl(Capability):
    capability_name: Literal["analyticsAcl"] = Field("analyticsAcl", exclude=True)
    actions: list[Literal["READ", "EXECUTE", "LIST"]]
    scope: AllScopeType


class AnnotationsAcl(Capability):
    capability_name: Literal["annotationsAcl"] = Field("annotationsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "SUGGEST", "REVIEW"]]
    scope: AllScopeType


class AppConfigAcl(Capability):
    capability_name: Literal["appConfigAcl"] = Field("appConfigAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrAppConfigScope


class AppHostingAcl(Capability):
    capability_name: Literal["appHostingAcl"] = Field("appHostingAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "RUN"]]
    scope: AllOrAppExternalIdScope


class AssetsAcl(Capability):
    capability_name: Literal["assetsAcl"] = Field("assetsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class ChartsAdminAcl(Capability):
    capability_name: Literal["chartsAdminAcl"] = Field("chartsAdminAcl", exclude=True)
    actions: list[Literal["READ", "UPDATE", "DELETE"]]
    scope: AllScopeType


class DataProductsAcl(Capability):
    """ACL for Data Products resources."""

    capability_name: Literal["dataProductsAcl"] = Field("dataProductsAcl", exclude=True)
    actions: list[Literal["CREATE", "READ", "UPDATE", "DELETE"]]
    scope: AllOrDataProductScope


class DataSetsAcl(Capability):
    capability_name: Literal["datasetsAcl"] = Field("datasetsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "OWNER"]]
    scope: AllOrIDScope


class DiagramParsingAcl(Capability):
    capability_name: Literal["diagramParsingAcl"] = Field("diagramParsingAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class DigitalTwinAcl(Capability):
    capability_name: Literal["digitalTwinAcl"] = Field("digitalTwinAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class EntityMatchingAcl(Capability):
    capability_name: Literal["entitymatchingAcl"] = Field("entitymatchingAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class EventsAcl(Capability):
    capability_name: Literal["eventsAcl"] = Field("eventsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class ExtractionPipelinesAcl(Capability):
    capability_name: Literal["extractionPipelinesAcl"] = Field("extractionPipelinesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllIDOrDataSetScope


class ExtractionsRunAcl(Capability):
    capability_name: Literal["extractionRunsAcl"] = Field("extractionRunsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllDataSetOrExtractionPipelineScope


class ExtractionConfigsAcl(Capability):
    capability_name: Literal["extractionConfigsAcl"] = Field("extractionConfigsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllDataSetOrExtractionPipelineScope


class FilesAcl(Capability):
    capability_name: Literal["filesAcl"] = Field("filesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class FunctionsAcl(Capability):
    capability_name: Literal["functionsAcl"] = Field("functionsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "RUN"]]
    scope: AllScopeType


class GeospatialAcl(Capability):
    capability_name: Literal["geospatialAcl"] = Field("geospatialAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class GeospatialCrsAcl(Capability):
    capability_name: Literal["geospatialCrsAcl"] = Field("geospatialCrsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class GroupsAcl(Capability):
    capability_name: Literal["groupsAcl"] = Field("groupsAcl", exclude=True)
    actions: list[Literal["CREATE", "DELETE", "READ", "LIST", "UPDATE"]]
    scope: AllOrCurrentUserScope


class LabelsAcl(Capability):
    capability_name: Literal["labelsAcl"] = Field("labelsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class LocationFiltersAcl(Capability):
    capability_name: Literal["locationFiltersAcl"] = Field("locationFiltersAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrIDScope


class ProjectsAcl(Capability):
    capability_name: Literal["projectsAcl"] = Field("projectsAcl", exclude=True)
    actions: list[Literal["READ", "CREATE", "LIST", "UPDATE", "DELETE"]]
    scope: AllScopeType


class RawAcl(Capability):
    capability_name: Literal["rawAcl"] = Field("rawAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "LIST"]]
    scope: AllOrTableScope


class RelationshipsAcl(Capability):
    capability_name: Literal["relationshipsAcl"] = Field("relationshipsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class RoboticsAcl(Capability):
    capability_name: Literal["roboticsAcl"] = Field("roboticsAcl", exclude=True)
    actions: list[Literal["READ", "CREATE", "UPDATE", "DELETE"]]
    scope: AllOrDataSetScope


class RuleSetsAcl(Capability):
    capability_name: Literal["ruleSetsAcl"] = Field("ruleSetsAcl", exclude=True)
    actions: list[Literal["CREATE", "READ", "UPDATE", "DELETE"]]
    scope: AllScopeType


class SAPWritebackAcl(Capability):
    capability_name: Literal["sapWritebackAcl"] = Field("sapWritebackAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrInstancesScope


class SAPWritebackRequestsAcl(Capability):
    capability_name: Literal["sapWritebackRequestsAcl"] = Field("sapWritebackRequestsAcl", exclude=True)
    actions: list[Literal["WRITE", "LIST"]]
    scope: AllOrInstancesScope


class SecurityCategoriesAcl(Capability):
    capability_name: Literal["securityCategoriesAcl"] = Field("securityCategoriesAcl", exclude=True)
    actions: list[Literal["MEMBEROF", "LIST", "CREATE", "UPDATE", "DELETE"]]
    scope: AllOrIDScopeLowerCase


class SeismicAcl(Capability):
    capability_name: Literal["seismicAcl"] = Field("seismicAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrPartitionScope


class SequencesAcl(Capability):
    capability_name: Literal["sequencesAcl"] = Field("sequencesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class SessionsAcl(Capability):
    capability_name: Literal["sessionsAcl"] = Field("sessionsAcl", exclude=True)
    actions: list[Literal["LIST", "CREATE", "DELETE"]]
    scope: AllScopeType


class ThreeDAcl(Capability):
    capability_name: Literal["threedAcl"] = Field("threedAcl", exclude=True)
    actions: list[Literal["READ", "CREATE", "UPDATE", "DELETE"]]
    scope: AllOrDataSetScope


class TimeSeriesAcl(Capability):
    capability_name: Literal["timeSeriesAcl"] = Field("timeSeriesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllDataSetIDOrAssetRootScope


class TimeSeriesSubscriptionsAcl(Capability):
    capability_name: Literal["timeSeriesSubscriptionsAcl"] = Field("timeSeriesSubscriptionsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class TransformationsAcl(Capability):
    capability_name: Literal["transformationsAcl"] = Field("transformationsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class TransformationsExternalDataSourcesAcl(Capability):
    capability_name: Literal["transformationsExternalDataSourcesAcl"] = Field(
        "transformationsExternalDataSourcesAcl", exclude=True
    )
    actions: list[Literal["READ", "WRITE", "USE"]]
    scope: AllOrDataSetScope


class TypesAcl(Capability):
    capability_name: Literal["typesAcl"] = Field("typesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class WellsAcl(Capability):
    capability_name: Literal["wellsAcl"] = Field("wellsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class ExperimentsAcl(Capability):
    capability_name: Literal["experimentAcl"] = Field("experimentAcl", exclude=True)
    actions: list[Literal["USE"]]
    scope: ExperimentScopeType


class TemplateGroupsAcl(Capability):
    capability_name: Literal["templateGroupsAcl"] = Field("templateGroupsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class TemplateInstancesAcl(Capability):
    capability_name: Literal["templateInstancesAcl"] = Field("templateInstancesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class DataModelInstancesAcl(Capability):
    capability_name: Literal["dataModelInstancesAcl"] = Field("dataModelInstancesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "WRITE_PROPERTIES"]]
    scope: AllOrSpaceIDScope


class DataModelsAcl(Capability):
    capability_name: Literal["dataModelsAcl"] = Field("dataModelsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrSpaceIDScope


class PipelinesAcl(Capability):
    capability_name: Literal["pipelinesAcl"] = Field("pipelinesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class DocumentPipelinesAcl(Capability):
    capability_name: Literal["documentPipelinesAcl"] = Field("documentPipelinesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class FilePipelinesAcl(Capability):
    capability_name: Literal["filePipelinesAcl"] = Field("filePipelinesAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class NotificationsAcl(Capability):
    capability_name: Literal["notificationsAcl"] = Field("notificationsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class ScheduledCalculationsAcl(Capability):
    capability_name: Literal["scheduledCalculationsAcl"] = Field("scheduledCalculationsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class MonitoringTasksAcl(Capability):
    capability_name: Literal["monitoringTasksAcl"] = Field("monitoringTasksAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class HostedExtractorsAcl(Capability):
    capability_name: Literal["hostedExtractorsAcl"] = Field("hostedExtractorsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class IntegrationConfigsAcl(Capability):
    capability_name: Literal["integrationConfigsAcl"] = Field("integrationConfigsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class IntegrationsAcl(Capability):
    capability_name: Literal["integrationsAcl"] = Field("integrationsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "USE"]]
    scope: AllScopeType


class VisionModelAcl(Capability):
    capability_name: Literal["visionModelAcl"] = Field("visionModelAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class DocumentFeedbackAcl(Capability):
    capability_name: Literal["documentFeedbackAcl"] = Field("documentFeedbackAcl", exclude=True)
    actions: list[Literal["CREATE", "READ", "DELETE"]]
    scope: AllScopeType


class WorkflowOrchestrationAcl(Capability):
    capability_name: Literal["workflowOrchestrationAcl"] = Field("workflowOrchestrationAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrDataSetScope


class PostgresGatewayAcl(Capability):
    capability_name: Literal["postgresGatewayAcl"] = Field("postgresGatewayAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrPostgresGatewayUsersScope


class UserProfilesAcl(Capability):
    capability_name: Literal["userProfilesAcl"] = Field("userProfilesAcl", exclude=True)
    actions: list[Literal["READ"]]
    scope: AllScopeType


class AuditlogAcl(Capability):
    capability_name: Literal["auditlogAcl"] = Field("auditlogAcl", exclude=True)
    actions: list[Literal["READ"]]
    scope: AllScopeType


class VideoStreamingAcl(Capability):
    capability_name: Literal["videoStreamingAcl"] = Field("videoStreamingAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "SUBSCRIBE", "PUBLISH"]]
    scope: AllOrDataSetScope


class LegacyModelHostingAcl(Capability):
    capability_name: Literal["modelHostingAcl"] = Field("modelHostingAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class LegacyGenericsAcl(Capability):
    capability_name: Literal["genericsAcl"] = Field("genericsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScopeType


class SimulatorsAcl(Capability):
    capability_name: Literal["simulatorsAcl"] = Field("simulatorsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE", "DELETE", "RUN", "MANAGE"]]
    scope: AllOrDataSetScope


class SubscribeSignalsAcl(Capability):
    capability_name: Literal["subscribeSignalsAcl"] = Field("subscribeSignalsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrCurrentUserScope


class StreamsAcl(Capability):
    capability_name: Literal["streamsAcl"] = Field("streamsAcl", exclude=True)
    actions: list[Literal["READ", "CREATE", "DELETE"]]
    scope: AllScopeType


class StreamRecordsAcl(Capability):
    capability_name: Literal["streamRecordsAcl"] = Field("streamRecordsAcl", exclude=True)
    actions: list[Literal["READ", "WRITE"]]
    scope: AllOrSpaceIDScope


class UnknownScope(Scope):
    """Wraps an unrecognised scope name; preserved for round-trip serialization."""

    scope_name: Literal["__unknown__"] = Field("__unknown__", exclude=True)
    unknown_name: str = Field(exclude=True)
    raw_data: dict[str, Any] = Field(exclude=True)

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def serialize_raw(self, handler: SerializerFunctionWrapHandler) -> dict:
        return self.raw_data

    @property
    def original_name(self) -> str:
        return self.unknown_name


ScopeType = Annotated[
    AgentExternalIdScope
    | AllScope
    | AppConfigScope
    | AppExternalIdScope
    | AssetRootIDScope
    | CurrentUserScope
    | DataProductScope
    | DataSetScope
    | ExperimentScope
    | ExtractionPipelineScope
    | IDScope
    | IDScopeLowerCase
    | InstancesScope
    | LegacyDataModelScope
    | LegacySpaceScope
    | PartitionScope
    | PostgresGatewayUsersScope
    | SpaceIDScope
    | TableScope
    | UnknownScope,
    Field(discriminator="scope_name"),
    BeforeValidator(_lift_scope_name_or_unknown),
]


class UnknownCapability(Capability):
    """Wraps an unrecognised capability name so ``GroupYAML`` validation succeeds.

    Scope-based dependencies (spaces, datasets, …) are still extracted normally.
    ``IDScope`` / ``IDScopeLowerCase`` are skipped — the resource type is implied
    by the capability name and cannot be inferred here.
    """

    capability_name: Literal["__unknown__"] = Field("__unknown__", exclude=True)
    unknown_name: str = Field(exclude=True)
    raw_data: dict[str, Any] = Field(exclude=True)
    scope: ScopeType = Field(default_factory=AllScope)
    actions: list[str] = Field(default_factory=list)

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def serialize_raw(self, handler: SerializerFunctionWrapHandler) -> dict:
        return self.raw_data

    @property
    def original_name(self) -> str:
        return self.unknown_name


CapabilityType = Annotated[
    AgentsAcl
    | AnalyticsAcl
    | AnnotationsAcl
    | AppConfigAcl
    | AppHostingAcl
    | AssetsAcl
    | AuditlogAcl
    | ChartsAdminAcl
    | DataModelInstancesAcl
    | DataModelsAcl
    | DataProductsAcl
    | DataSetsAcl
    | DiagramParsingAcl
    | DigitalTwinAcl
    | DocumentFeedbackAcl
    | DocumentPipelinesAcl
    | EntityMatchingAcl
    | EventsAcl
    | ExperimentsAcl
    | ExtractionConfigsAcl
    | ExtractionPipelinesAcl
    | ExtractionsRunAcl
    | FilePipelinesAcl
    | FilesAcl
    | FunctionsAcl
    | GeospatialAcl
    | GeospatialCrsAcl
    | GroupsAcl
    | HostedExtractorsAcl
    | IntegrationConfigsAcl
    | IntegrationsAcl
    | LabelsAcl
    | LegacyGenericsAcl
    | LegacyModelHostingAcl
    | LocationFiltersAcl
    | MonitoringTasksAcl
    | NotificationsAcl
    | PipelinesAcl
    | PostgresGatewayAcl
    | ProjectsAcl
    | RawAcl
    | RelationshipsAcl
    | RoboticsAcl
    | RuleSetsAcl
    | SAPWritebackAcl
    | SAPWritebackRequestsAcl
    | ScheduledCalculationsAcl
    | SecurityCategoriesAcl
    | SeismicAcl
    | SequencesAcl
    | SessionsAcl
    | SimulatorsAcl
    | StreamRecordsAcl
    | StreamsAcl
    | SubscribeSignalsAcl
    | TemplateGroupsAcl
    | TemplateInstancesAcl
    | ThreeDAcl
    | TimeSeriesAcl
    | TimeSeriesSubscriptionsAcl
    | TransformationsAcl
    | TransformationsExternalDataSourcesAcl
    | TypesAcl
    | UnknownCapability
    | UserProfilesAcl
    | VideoStreamingAcl
    | VisionModelAcl
    | WellsAcl
    | WorkflowOrchestrationAcl,
    Field(discriminator="capability_name"),
    BeforeValidator(_lift_capability_name_or_unknown),
]

_KNOWN_SCOPE_NAMES.update(
    scope.model_fields["scope_name"].default
    for scope in get_concrete_subclasses(Scope)
    if scope.model_fields["scope_name"].default != "__unknown__"
)
_KNOWN_CAPABILITY_NAMES.update(
    capability.model_fields["capability_name"].default
    for capability in get_concrete_subclasses(Capability)
    if capability.model_fields["capability_name"].default != "__unknown__"
)
