import sys
from types import MappingProxyType, UnionType
from typing import Any, ClassVar, Literal, cast, get_args

from pydantic import ModelWrapValidatorHandler, field_validator, model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self

from .base import BaseModelResource


class Scope(BaseModelResource):
    _scope_name: ClassVar[str]

    @model_validator(mode="wrap")
    @classmethod
    def find_scope_cls(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, Scope):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid scope data '{type(data)}' expected dict")

        if cls is not Scope:
            return handler(data)
        name, content = next(iter(data.items()))
        if name not in _SCOPE_CLASS_BY_NAME:
            raise ValueError(
                f"invalid scope name '{name}'. Expected one of {humanize_collection(_SCOPE_CLASS_BY_NAME.keys(), bind_word='or')}"
            )
        cls_ = _SCOPE_CLASS_BY_NAME[name]
        return cast(Self, cls_.model_validate(content))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_scope_name(self, handler: SerializerFunctionWrapHandler) -> dict:
        if self._scope_name is None:
            raise ValueError("Scope name is not set")
        serialized_data = handler(self)
        return {self._scope_name: serialized_data}


class AllScope(Scope):
    _scope_name = "all"


class AppConfigScope(Scope):
    _scope_name = "appScope"
    apps: list[Literal["SEARCH"]]


class CurrentUserScope(Scope):
    _scope_name = "currentuserscope"


class IDScope(Scope):
    _scope_name = "idScope"
    ids: list[str]


class IDScopeLowerCase(Scope):
    """Necessary due to lack of API standardisation on scope name: 'idScope' VS 'idscope'"""

    _scope_name = "idscope"
    ids: list[str]


class InstancesScope(Scope):
    _scope_name = "instancesScope"
    instances: list[str]


class ExtractionPipelineScope(Scope):
    _scope_name = "extractionPipelineScope"
    ids: list[str]


class PostgresGatewayUsersScope(Scope):
    _scope_name = "usersScope"
    usernames: list[str]


class DataSetScope(Scope):
    _scope_name = "datasetScope"
    ids: list[str]


class TableScope(Scope):
    _scope_name = "tableScope"
    dbs_to_tables: dict[str, list[str]]


class AssetRootIDScope(Scope):
    _scope_name = "assetRootIdScope"
    root_ids: list[str]


class ExperimentScope(Scope):
    _scope_name = "experimentscope"
    experiments: list[str]


class SpaceIDScope(Scope):
    _scope_name = "spaceIdScope"
    space_ids: list[str]


class PartitionScope(Scope):
    _scope_name = "partition"
    partition_ids: list[int]


class LegacySpaceScope(Scope):
    _scope_name = "spaceScope"
    external_ids: list[str]


class LegacyDataModelScope(Scope):
    _scope_name = "dataModelScope"
    external_ids: list[str]


class Capability(BaseModelResource):
    _capability_name: ClassVar[str]
    scope: Scope

    @field_validator("scope", mode="before")
    @classmethod
    def find_scope_cls(cls, data: Any) -> Scope:
        annotation = cls.model_fields["scope"].annotation
        if isinstance(annotation, UnionType):
            valid_types = {s._scope_name for s in get_args(annotation)}
        elif annotation is not None and issubclass(annotation, Scope):
            valid_types = {annotation._scope_name}
        else:
            raise ValueError(f"Invalid scope annotation '{annotation}'")

        name = next(iter(data.keys()))
        if name not in valid_types:
            raise ValueError(
                f"invalid scope name '{name}'. Expected {humanize_collection(valid_types, bind_word='or')}"
            )

        return Scope.model_validate(data)

    @model_validator(mode="wrap")
    @classmethod
    def find_capability_cls(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if cls is not Capability:
            return handler(data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid capability data '{type(data)}' expected dict")
        name, content = next(iter(data.items()))
        if name not in _CAPABILITY_CLASS_BY_NAME:
            raise ValueError(f"Invalid capability name '{name}'. Expected one of {_CAPABILITY_CLASS_BY_NAME.keys()}")
        cls_ = _CAPABILITY_CLASS_BY_NAME[name]
        return cast(Self, cls_.model_validate(content))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_capability_name(self, handler: SerializerFunctionWrapHandler) -> dict:
        if self._capability_name is None:
            raise ValueError("Capability name is not set")
        serialized_data = handler(self)
        return {self._capability_name: serialized_data}


class AgentsAcl(Capability):
    _capability_name = "agentsAcl"
    actions: list[Literal["READ", "WRITE", "RUN"]]
    scope: AllScope


class AnalyticsAcl(Capability):
    _capability_name = "analyticsAcl"
    actions: list[Literal["READ", "EXECUTE", "LIST"]]
    scope: AllScope


class AnnotationsAcl(Capability):
    _capability_name = "annotationsAcl"
    actions: list[Literal["READ", "WRITE", "SUGGEST", "REVIEW"]]
    scope: AllScope


class AppConfigAcl(Capability):
    _capability_name = "appConfigAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | AppConfigScope


class AssetsAcl(Capability):
    _capability_name = "assetsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class DataSetsAcl(Capability):
    _capability_name = "datasetsAcl"
    actions: list[Literal["READ", "WRITE", "OWNER"]]
    scope: AllScope | IDScope


class DiagramParsingAcl(Capability):
    _capability_name = "diagramParsingAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class DigitalTwinAcl(Capability):
    _capability_name = "digitalTwinAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class EntityMatchingAcl(Capability):
    _capability_name = "entitymatchingAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class EventsAcl(Capability):
    _capability_name = "eventsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class ExtractionPipelinesAcl(Capability):
    _capability_name = "extractionPipelinesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | IDScope | DataSetScope


class ExtractionsRunAcl(Capability):
    _capability_name = "extractionRunsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope | ExtractionPipelineScope


class ExtractionConfigsAcl(Capability):
    _capability_name = "extractionConfigsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope | ExtractionPipelineScope


class FilesAcl(Capability):
    _capability_name = "filesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class FunctionsAcl(Capability):
    _capability_name = "functionsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class GeospatialAcl(Capability):
    _capability_name = "geospatialAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class GeospatialCrsAcl(Capability):
    _capability_name = "geospatialCrsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class GroupsAcl(Capability):
    _capability_name = "groupsAcl"
    actions: list[Literal["CREATE", "DELETE", "READ", "LIST", "UPDATE"]]
    scope: AllScope | CurrentUserScope


class LabelsAcl(Capability):
    _capability_name = "labelsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class LocationFiltersAcl(Capability):
    _capability_name = "locationFiltersAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | IDScope


class ProjectsAcl(Capability):
    _capability_name = "projectsAcl"
    actions: list[Literal["READ", "CREATE", "LIST", "UPDATE", "DELETE"]]
    scope: AllScope


class RawAcl(Capability):
    _capability_name = "rawAcl"
    actions: list[Literal["READ", "WRITE", "LIST"]]
    scope: AllScope | TableScope


class RelationshipsAcl(Capability):
    _capability_name = "relationshipsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class RoboticsAcl(Capability):
    _capability_name = "roboticsAcl"
    actions: list[Literal["READ", "CREATE", "UPDATE", "DELETE"]]
    scope: AllScope | DataSetScope


class SAPWritebackAcl(Capability):
    _capability_name = "sapWritebackAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | InstancesScope


class SAPWritebackRequestsAcl(Capability):
    _capability_name = "sapWritebackRequestsAcl"
    actions: list[Literal["WRITE", "LIST"]]
    scope: AllScope | InstancesScope


class SecurityCategoriesAcl(Capability):
    _capability_name = "securityCategoriesAcl"
    actions: list[Literal["MEMBEROF", "LIST", "CREATE", "UPDATE", "DELETE"]]
    scope: AllScope | IDScopeLowerCase


class SeismicAcl(Capability):
    _capability_name = "seismicAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | PartitionScope


class SequencesAcl(Capability):
    _capability_name = "sequencesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class SessionsAcl(Capability):
    _capability_name = "sessionsAcl"
    actions: list[Literal["LIST", "CREATE", "DELETE"]]
    scope: AllScope


class ThreeDAcl(Capability):
    _capability_name = "threedAcl"
    actions: list[Literal["READ", "CREATE", "UPDATE", "DELETE"]]
    scope: AllScope | DataSetScope


class TimeSeriesAcl(Capability):
    _capability_name = "timeSeriesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope | IDScopeLowerCase | AssetRootIDScope


class TimeSeriesSubscriptionsAcl(Capability):
    _capability_name = "timeSeriesSubscriptionsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class TransformationsAcl(Capability):
    _capability_name = "transformationsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class TypesAcl(Capability):
    _capability_name = "typesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class WellsAcl(Capability):
    _capability_name = "wellsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class ExperimentsAcl(Capability):
    _capability_name = "experimentAcl"
    actions: list[Literal["USE"]]
    scope: ExperimentScope


class TemplateGroupsAcl(Capability):
    _capability_name = "templateGroupsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class TemplateInstancesAcl(Capability):
    _capability_name = "templateInstancesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class DataModelInstancesAcl(Capability):
    _capability_name = "dataModelInstancesAcl"
    actions: list[Literal["READ", "WRITE", "WRITE_PROPERTIES"]]
    scope: AllScope | SpaceIDScope


class DataModelsAcl(Capability):
    _capability_name = "dataModelsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | SpaceIDScope


class PipelinesAcl(Capability):
    _capability_name = "pipelinesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class DocumentPipelinesAcl(Capability):
    _capability_name = "documentPipelinesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class FilePipelinesAcl(Capability):
    _capability_name = "filePipelinesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class NotificationsAcl(Capability):
    _capability_name = "notificationsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class ScheduledCalculationsAcl(Capability):
    _capability_name = "scheduledCalculationsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class MonitoringTasksAcl(Capability):
    _capability_name = "monitoringTasksAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class HostedExtractorsAcl(Capability):
    _capability_name = "hostedExtractorsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class VisionModelAcl(Capability):
    _capability_name = "visionModelAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class DocumentFeedbackAcl(Capability):
    _capability_name = "documentFeedbackAcl"
    actions: list[Literal["CREATE", "READ", "DELETE"]]
    scope: AllScope


class WorkflowOrchestrationAcl(Capability):
    _capability_name = "workflowOrchestrationAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class PostgresGatewayAcl(Capability):
    _capability_name = "postgresGatewayAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | PostgresGatewayUsersScope


class UserProfilesAcl(Capability):
    _capability_name = "userProfilesAcl"
    actions: list[Literal["READ"]]
    scope: AllScope


class AuditlogAcl(Capability):
    _capability_name = "auditlogAcl"
    actions: list[Literal["READ"]]
    scope: AllScope


class VideoStreamingAcl(Capability):
    _capability_name = "videoStreamingAcl"
    actions: list[Literal["READ", "WRITE", "SUBSCRIBE", "PUBLISH"]]
    scope: AllScope | DataSetScope


class LegacyModelHostingAcl(Capability):
    _capability_name = "modelHostingAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class LegacyGenericsAcl(Capability):
    _capability_name = "genericsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


_CAPABILITY_CLASS_BY_NAME: MappingProxyType[str, type[Capability]] = MappingProxyType(
    {c._capability_name: c for c in Capability.__subclasses__()}
)
ALL_CAPABILITIES = sorted(_CAPABILITY_CLASS_BY_NAME)

_SCOPE_CLASS_BY_NAME: MappingProxyType[str, type[Scope]] = MappingProxyType(
    {s._scope_name: s for s in Scope.__subclasses__()}
)

_SCOPE_BY_CLASS_NAME: MappingProxyType[str, str] = MappingProxyType(
    {scope_cls.__name__: scope_name for scope_name, scope_cls in _SCOPE_CLASS_BY_NAME.items()}
)
