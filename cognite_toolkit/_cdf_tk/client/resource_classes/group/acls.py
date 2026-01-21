"""ACL (Access Control Sequence) definitions for Group capabilities.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from collections.abc import Sequence
from typing import Annotated, Any, Literal, TypeAlias

from pydantic import BeforeValidator, Field, TypeAdapter, model_serializer, model_validator
from pydantic_core.core_schema import FieldSerializationInfo

from cognite_toolkit._cdf_tk.client._resource_base import BaseModelObject
from cognite_toolkit._cdf_tk.client.resource_classes.group._constants import ACL_NAME, SCOPE_NAME
from cognite_toolkit._cdf_tk.utils._auxiliary import get_concrete_subclasses

from .scopes import (
    AllScope,
    AppConfigScope,
    AssetRootIDScope,
    CurrentUserScope,
    DataSetScope,
    ExperimentScope,
    ExtractionPipelineScope,
    IDScope,
    IDScopeLowerCase,
    InstancesScope,
    PartitionScope,
    PostgresGatewayUsersScope,
    Scope,
    SpaceIDScope,
    TableScope,
)


class Acl(BaseModelObject):
    """Base class for all ACL (Access Control Sequence) definitions."""

    acl_name: str
    actions: Sequence[str]
    scope: Scope

    @model_validator(mode="before")
    @classmethod
    def convert_scope_format(cls, value: Any) -> Any:
        """Convert scope from API format {'all': {}} to model format {'scope_name': 'all'}."""
        if not isinstance(value, dict):
            return value
        scope = value.get("scope")
        if not isinstance(scope, dict):
            return value
        # If scope already has scope_name, it's in the correct format
        if SCOPE_NAME in scope:
            return value
        # Convert from API format: find the scope key and extract its data
        scope_name = next(iter(scope.keys()), None)
        if scope_name is not None:
            scope_data = scope.get(scope_name, {})
            if isinstance(scope_data, dict):
                new_scope = {SCOPE_NAME: scope_name, **scope_data}
                value = dict(value)
                value["scope"] = new_scope
        return value

    # MyPy complains that info; FieldSerializationInfo is not compatible with info: Any
    # It is.
    @model_serializer  # type: ignore[type-var]
    def convert_scope_to_api_format(self, info: FieldSerializationInfo) -> dict[str, Any]:
        """Convert scope from model format {'scope_name': 'all'} to API format {'all': {}}."""
        output: dict[str, Any] = {"actions": self.actions}
        scope = self.scope.model_dump(**vars(info))
        if isinstance(scope, dict):
            output["scope"] = {self.scope.scope_name: scope}
        return output


class AgentsAcl(Acl):
    """ACL for Agents resources."""

    acl_name: Literal["agentsAcl"] = Field("agentsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE", "RUN"]]
    scope: AllScope


class AnalyticsAcl(Acl):
    """ACL for Analytics resources."""

    acl_name: Literal["analyticsAcl"] = Field("analyticsAcl", exclude=True)
    actions: Sequence[Literal["READ", "EXECUTE", "LIST"]]
    scope: AllScope


class AnnotationsAcl(Acl):
    """ACL for Annotations resources."""

    acl_name: Literal["annotationsAcl"] = Field("annotationsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE", "SUGGEST", "REVIEW"]]
    scope: AllScope


class AppConfigAcl(Acl):
    """ACL for App Config resources."""

    acl_name: Literal["appConfigAcl"] = Field("appConfigAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | AppConfigScope


class AssetsAcl(Acl):
    """ACL for Assets resources."""

    acl_name: Literal["assetsAcl"] = Field("assetsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class AuditlogAcl(Acl):
    """ACL for Audit Log resources."""

    acl_name: Literal["auditlogAcl"] = Field("auditlogAcl", exclude=True)
    actions: Sequence[Literal["READ"]]
    scope: AllScope


class DataModelInstancesAcl(Acl):
    """ACL for Data Model Instances resources."""

    acl_name: Literal["dataModelInstancesAcl"] = Field("dataModelInstancesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE", "WRITE_PROPERTIES"]]
    scope: AllScope | SpaceIDScope


class DataModelsAcl(Acl):
    """ACL for Data Models resources."""

    acl_name: Literal["dataModelsAcl"] = Field("dataModelsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | SpaceIDScope


class DataSetsAcl(Acl):
    """ACL for Data Sets resources."""

    acl_name: Literal["datasetsAcl"] = Field("datasetsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE", "OWNER"]]
    scope: AllScope | IDScope


class DiagramParsingAcl(Acl):
    """ACL for Diagram Parsing resources."""

    acl_name: Literal["diagramParsingAcl"] = Field("diagramParsingAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class DigitalTwinAcl(Acl):
    """ACL for Digital Twin resources."""

    acl_name: Literal["digitalTwinAcl"] = Field("digitalTwinAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class DocumentFeedbackAcl(Acl):
    """ACL for Document Feedback resources."""

    acl_name: Literal["documentFeedbackAcl"] = Field("documentFeedbackAcl", exclude=True)
    actions: Sequence[Literal["CREATE", "READ", "DELETE"]]
    scope: AllScope


class DocumentPipelinesAcl(Acl):
    """ACL for Document Pipelines resources."""

    acl_name: Literal["documentPipelinesAcl"] = Field("documentPipelinesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class EntityMatchingAcl(Acl):
    """ACL for Entity Matching resources."""

    acl_name: Literal["entitymatchingAcl"] = Field("entitymatchingAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class EventsAcl(Acl):
    """ACL for Events resources."""

    acl_name: Literal["eventsAcl"] = Field("eventsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class ExperimentsAcl(Acl):
    """ACL for Experiments resources."""

    acl_name: Literal["experimentAcl"] = Field("experimentAcl", exclude=True)
    actions: Sequence[Literal["USE"]]
    scope: ExperimentScope


class ExtractionConfigsAcl(Acl):
    """ACL for Extraction Configs resources."""

    acl_name: Literal["extractionConfigsAcl"] = Field("extractionConfigsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope | ExtractionPipelineScope


class ExtractionPipelinesAcl(Acl):
    """ACL for Extraction Pipelines resources."""

    acl_name: Literal["extractionPipelinesAcl"] = Field("extractionPipelinesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | IDScope | DataSetScope


class ExtractionRunsAcl(Acl):
    """ACL for Extraction Runs resources."""

    acl_name: Literal["extractionRunsAcl"] = Field("extractionRunsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope | ExtractionPipelineScope


class FilePipelinesAcl(Acl):
    """ACL for File Pipelines resources."""

    acl_name: Literal["filePipelinesAcl"] = Field("filePipelinesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class FilesAcl(Acl):
    """ACL for Files resources."""

    acl_name: Literal["filesAcl"] = Field("filesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class FunctionsAcl(Acl):
    """ACL for Functions resources."""

    acl_name: Literal["functionsAcl"] = Field("functionsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class GeospatialAcl(Acl):
    """ACL for Geospatial resources."""

    acl_name: Literal["geospatialAcl"] = Field("geospatialAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class GeospatialCrsAcl(Acl):
    """ACL for Geospatial CRS resources."""

    acl_name: Literal["geospatialCrsAcl"] = Field("geospatialCrsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class GroupsAcl(Acl):
    """ACL for Groups resources."""

    acl_name: Literal["groupsAcl"] = Field("groupsAcl", exclude=True)
    actions: Sequence[Literal["CREATE", "DELETE", "READ", "LIST", "UPDATE"]]
    scope: AllScope | CurrentUserScope


class HostedExtractorsAcl(Acl):
    """ACL for Hosted Extractors resources."""

    acl_name: Literal["hostedExtractorsAcl"] = Field("hostedExtractorsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class LabelsAcl(Acl):
    """ACL for Labels resources."""

    acl_name: Literal["labelsAcl"] = Field("labelsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class LegacyGenericsAcl(Acl):
    """ACL for Legacy Generics resources."""

    acl_name: Literal["genericsAcl"] = Field("genericsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class LegacyModelHostingAcl(Acl):
    """ACL for Legacy Model Hosting resources."""

    acl_name: Literal["modelHostingAcl"] = Field("modelHostingAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class LocationFiltersAcl(Acl):
    """ACL for Location Filters resources."""

    acl_name: Literal["locationFiltersAcl"] = Field("locationFiltersAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | IDScope


class MonitoringTasksAcl(Acl):
    """ACL for Monitoring Tasks resources."""

    acl_name: Literal["monitoringTasksAcl"] = Field("monitoringTasksAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class NotificationsAcl(Acl):
    """ACL for Notifications resources."""

    acl_name: Literal["notificationsAcl"] = Field("notificationsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class PipelinesAcl(Acl):
    """ACL for Pipelines resources."""

    acl_name: Literal["pipelinesAcl"] = Field("pipelinesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class PostgresGatewayAcl(Acl):
    """ACL for PostgreSQL Gateway resources."""

    acl_name: Literal["postgresGatewayAcl"] = Field("postgresGatewayAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | PostgresGatewayUsersScope


class ProjectsAcl(Acl):
    """ACL for Projects resources."""

    acl_name: Literal["projectsAcl"] = Field("projectsAcl", exclude=True)
    actions: Sequence[Literal["READ", "CREATE", "LIST", "UPDATE", "DELETE"]]
    scope: AllScope


class RawAcl(Acl):
    """ACL for RAW resources."""

    acl_name: Literal["rawAcl"] = Field("rawAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE", "LIST"]]
    scope: AllScope | TableScope


class RelationshipsAcl(Acl):
    """ACL for Relationships resources."""

    acl_name: Literal["relationshipsAcl"] = Field("relationshipsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class RoboticsAcl(Acl):
    """ACL for Robotics resources."""

    acl_name: Literal["roboticsAcl"] = Field("roboticsAcl", exclude=True)
    actions: Sequence[Literal["READ", "CREATE", "UPDATE", "DELETE"]]
    scope: AllScope | DataSetScope


class SAPWritebackAcl(Acl):
    """ACL for SAP Writeback resources."""

    acl_name: Literal["sapWritebackAcl"] = Field("sapWritebackAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | InstancesScope


class SAPWritebackRequestsAcl(Acl):
    """ACL for SAP Writeback Requests resources."""

    acl_name: Literal["sapWritebackRequestsAcl"] = Field("sapWritebackRequestsAcl", exclude=True)
    actions: Sequence[Literal["WRITE", "LIST"]]
    scope: AllScope | InstancesScope


class ScheduledCalculationsAcl(Acl):
    """ACL for Scheduled Calculations resources."""

    acl_name: Literal["scheduledCalculationsAcl"] = Field("scheduledCalculationsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class SecurityCategoriesAcl(Acl):
    """ACL for Security Categories resources."""

    acl_name: Literal["securityCategoriesAcl"] = Field("securityCategoriesAcl", exclude=True)
    actions: Sequence[Literal["MEMBEROF", "LIST", "CREATE", "UPDATE", "DELETE"]]
    scope: AllScope | IDScopeLowerCase


class SeismicAcl(Acl):
    """ACL for Seismic resources."""

    acl_name: Literal["seismicAcl"] = Field("seismicAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | PartitionScope


class SequencesAcl(Acl):
    """ACL for Sequences resources."""

    acl_name: Literal["sequencesAcl"] = Field("sequencesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class SessionsAcl(Acl):
    """ACL for Sessions resources."""

    acl_name: Literal["sessionsAcl"] = Field("sessionsAcl", exclude=True)
    actions: Sequence[Literal["LIST", "CREATE", "DELETE"]]
    scope: AllScope


class StreamRecordsAcl(Acl):
    """ACL for Stream Records resources."""

    acl_name: Literal["streamRecordsAcl"] = Field("streamRecordsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | SpaceIDScope


class StreamsAcl(Acl):
    """ACL for Streams resources."""

    acl_name: Literal["streamsAcl"] = Field("streamsAcl", exclude=True)
    actions: Sequence[Literal["READ", "CREATE", "DELETE"]]
    scope: AllScope


class TemplateGroupsAcl(Acl):
    """ACL for Template Groups resources."""

    acl_name: Literal["templateGroupsAcl"] = Field("templateGroupsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class TemplateInstancesAcl(Acl):
    """ACL for Template Instances resources."""

    acl_name: Literal["templateInstancesAcl"] = Field("templateInstancesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class ThreeDAcl(Acl):
    """ACL for 3D resources."""

    acl_name: Literal["threedAcl"] = Field("threedAcl", exclude=True)
    actions: Sequence[Literal["READ", "CREATE", "UPDATE", "DELETE"]]
    scope: AllScope | DataSetScope


class TimeSeriesAcl(Acl):
    """ACL for Time Series resources."""

    acl_name: Literal["timeSeriesAcl"] = Field("timeSeriesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope | IDScopeLowerCase | AssetRootIDScope


class TimeSeriesSubscriptionsAcl(Acl):
    """ACL for Time Series Subscriptions resources."""

    acl_name: Literal["timeSeriesSubscriptionsAcl"] = Field("timeSeriesSubscriptionsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class TransformationsAcl(Acl):
    """ACL for Transformations resources."""

    acl_name: Literal["transformationsAcl"] = Field("transformationsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class TypesAcl(Acl):
    """ACL for Types resources."""

    acl_name: Literal["typesAcl"] = Field("typesAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class UserProfilesAcl(Acl):
    """ACL for User Profiles resources."""

    acl_name: Literal["userProfilesAcl"] = Field("userProfilesAcl", exclude=True)
    actions: Sequence[Literal["READ"]]
    scope: AllScope


class VideoStreamingAcl(Acl):
    """ACL for Video Streaming resources."""

    acl_name: Literal["videoStreamingAcl"] = Field("videoStreamingAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE", "SUBSCRIBE", "PUBLISH"]]
    scope: AllScope | DataSetScope


class VisionModelAcl(Acl):
    """ACL for Vision Model resources."""

    acl_name: Literal["visionModelAcl"] = Field("visionModelAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class WellsAcl(Acl):
    """ACL for Wells resources."""

    acl_name: Literal["wellsAcl"] = Field("wellsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope


class WorkflowOrchestrationAcl(Acl):
    """ACL for Workflow Orchestration resources."""

    acl_name: Literal["workflowOrchestrationAcl"] = Field("workflowOrchestrationAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class SimulatorsAcl(Acl):
    """ACL for Simulators resources."""

    acl_name: Literal["simulatorsAcl"] = Field("simulatorsAcl", exclude=True)
    actions: Sequence[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class UnknownAcl(Acl):
    """Fallback for unknown ACL types."""

    acl_name: Literal["unknownAcl"] = Field("unknownAcl", exclude=True)
    actions: Sequence[str]
    scope: AllScope


def _get_acl_name(cls: type[Acl]) -> str | None:
    """Get the acl_name default value from a Pydantic model class."""
    field = cls.model_fields.get("acl_name")
    if field is not None and field.default is not None:
        return field.default
    return None


_KNOWN_ACLS = {
    name: acl
    for acl in get_concrete_subclasses(Acl)
    if (name := _get_acl_name(acl)) is not None and name != "unknownAcl"
}


def _handle_unknown_acl(value: Any) -> Any:
    if isinstance(value, dict) and isinstance(acl_name := value[ACL_NAME], str):
        acl_class = _KNOWN_ACLS.get(acl_name)
        if acl_class:
            return TypeAdapter(acl_class).validate_python(value)
    return UnknownAcl.model_validate(value)


AclType: TypeAlias = Annotated[
    (
        AgentsAcl
        | AnalyticsAcl
        | AnnotationsAcl
        | AppConfigAcl
        | AssetsAcl
        | AuditlogAcl
        | DataModelInstancesAcl
        | DataModelsAcl
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
        | ExtractionRunsAcl
        | FilePipelinesAcl
        | FilesAcl
        | FunctionsAcl
        | GeospatialAcl
        | GeospatialCrsAcl
        | GroupsAcl
        | HostedExtractorsAcl
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
        | SAPWritebackAcl
        | SAPWritebackRequestsAcl
        | ScheduledCalculationsAcl
        | SecurityCategoriesAcl
        | SeismicAcl
        | SequencesAcl
        | SessionsAcl
        | StreamRecordsAcl
        | StreamsAcl
        | TemplateGroupsAcl
        | TemplateInstancesAcl
        | ThreeDAcl
        | TimeSeriesAcl
        | TimeSeriesSubscriptionsAcl
        | TransformationsAcl
        | TypesAcl
        | UserProfilesAcl
        | VideoStreamingAcl
        | VisionModelAcl
        | WellsAcl
        | WorkflowOrchestrationAcl
        | SimulatorsAcl
        | UnknownAcl
    ),
    BeforeValidator(_handle_unknown_acl),
]
