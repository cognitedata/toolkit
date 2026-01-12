"""ACL (Access Control List) definitions for Group capabilities.

Based on the API specification at:
https://api-docs.cognite.com/20230101/tag/Groups/operation/createGroups
"""

from types import MappingProxyType
from typing import Any, ClassVar, Literal

from pydantic import model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.client.data_classes.base import BaseModelObject

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
    parse_scope,
)


class Acl(BaseModelObject):
    """Base class for all ACL (Access Control List) definitions."""

    _acl_name: ClassVar[str]
    actions: list[str]
    scope: Scope

    @model_validator(mode="before")
    @classmethod
    def parse_scope(cls, data: Any) -> Any:
        if isinstance(data, dict) and "scope" in data:
            scope_data = data["scope"]
            if isinstance(scope_data, dict):
                data = dict(data)
                data["scope"] = parse_scope(scope_data)
        return data

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_acl_name(self, handler: SerializerFunctionWrapHandler) -> dict:
        if self._acl_name is None:
            raise ValueError("ACL name is not set")
        serialized_data = handler(self)
        return {self._acl_name: serialized_data}


class AgentsAcl(Acl):
    """ACL for Agents resources."""

    _acl_name: ClassVar[str] = "agentsAcl"
    actions: list[Literal["READ", "WRITE", "RUN"]]
    scope: AllScope


class AnalyticsAcl(Acl):
    """ACL for Analytics resources."""

    _acl_name: ClassVar[str] = "analyticsAcl"
    actions: list[Literal["READ", "EXECUTE", "LIST"]]
    scope: AllScope


class AnnotationsAcl(Acl):
    """ACL for Annotations resources."""

    _acl_name: ClassVar[str] = "annotationsAcl"
    actions: list[Literal["READ", "WRITE", "SUGGEST", "REVIEW"]]
    scope: AllScope


class AppConfigAcl(Acl):
    """ACL for App Config resources."""

    _acl_name: ClassVar[str] = "appConfigAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | AppConfigScope


class AssetsAcl(Acl):
    """ACL for Assets resources."""

    _acl_name: ClassVar[str] = "assetsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class AuditlogAcl(Acl):
    """ACL for Audit Log resources."""

    _acl_name: ClassVar[str] = "auditlogAcl"
    actions: list[Literal["READ"]]
    scope: AllScope


class DataModelInstancesAcl(Acl):
    """ACL for Data Model Instances resources."""

    _acl_name: ClassVar[str] = "dataModelInstancesAcl"
    actions: list[Literal["READ", "WRITE", "WRITE_PROPERTIES"]]
    scope: AllScope | SpaceIDScope


class DataModelsAcl(Acl):
    """ACL for Data Models resources."""

    _acl_name: ClassVar[str] = "dataModelsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | SpaceIDScope


class DataSetsAcl(Acl):
    """ACL for Data Sets resources."""

    _acl_name: ClassVar[str] = "datasetsAcl"
    actions: list[Literal["READ", "WRITE", "OWNER"]]
    scope: AllScope | IDScope


class DiagramParsingAcl(Acl):
    """ACL for Diagram Parsing resources."""

    _acl_name: ClassVar[str] = "diagramParsingAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class DigitalTwinAcl(Acl):
    """ACL for Digital Twin resources."""

    _acl_name: ClassVar[str] = "digitalTwinAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class DocumentFeedbackAcl(Acl):
    """ACL for Document Feedback resources."""

    _acl_name: ClassVar[str] = "documentFeedbackAcl"
    actions: list[Literal["CREATE", "READ", "DELETE"]]
    scope: AllScope


class DocumentPipelinesAcl(Acl):
    """ACL for Document Pipelines resources."""

    _acl_name: ClassVar[str] = "documentPipelinesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class EntityMatchingAcl(Acl):
    """ACL for Entity Matching resources."""

    _acl_name: ClassVar[str] = "entitymatchingAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class EventsAcl(Acl):
    """ACL for Events resources."""

    _acl_name: ClassVar[str] = "eventsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class ExperimentsAcl(Acl):
    """ACL for Experiments resources."""

    _acl_name: ClassVar[str] = "experimentAcl"
    actions: list[Literal["USE"]]
    scope: ExperimentScope


class ExtractionConfigsAcl(Acl):
    """ACL for Extraction Configs resources."""

    _acl_name: ClassVar[str] = "extractionConfigsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope | ExtractionPipelineScope


class ExtractionPipelinesAcl(Acl):
    """ACL for Extraction Pipelines resources."""

    _acl_name: ClassVar[str] = "extractionPipelinesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | IDScope | DataSetScope


class ExtractionRunsAcl(Acl):
    """ACL for Extraction Runs resources."""

    _acl_name: ClassVar[str] = "extractionRunsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope | ExtractionPipelineScope


class FilePipelinesAcl(Acl):
    """ACL for File Pipelines resources."""

    _acl_name: ClassVar[str] = "filePipelinesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class FilesAcl(Acl):
    """ACL for Files resources."""

    _acl_name: ClassVar[str] = "filesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class FunctionsAcl(Acl):
    """ACL for Functions resources."""

    _acl_name: ClassVar[str] = "functionsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class GeospatialAcl(Acl):
    """ACL for Geospatial resources."""

    _acl_name: ClassVar[str] = "geospatialAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class GeospatialCrsAcl(Acl):
    """ACL for Geospatial CRS resources."""

    _acl_name: ClassVar[str] = "geospatialCrsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class GroupsAcl(Acl):
    """ACL for Groups resources."""

    _acl_name: ClassVar[str] = "groupsAcl"
    actions: list[Literal["CREATE", "DELETE", "READ", "LIST", "UPDATE"]]
    scope: AllScope | CurrentUserScope


class HostedExtractorsAcl(Acl):
    """ACL for Hosted Extractors resources."""

    _acl_name: ClassVar[str] = "hostedExtractorsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class LabelsAcl(Acl):
    """ACL for Labels resources."""

    _acl_name: ClassVar[str] = "labelsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class LegacyGenericsAcl(Acl):
    """ACL for Legacy Generics resources."""

    _acl_name: ClassVar[str] = "genericsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class LegacyModelHostingAcl(Acl):
    """ACL for Legacy Model Hosting resources."""

    _acl_name: ClassVar[str] = "modelHostingAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class LocationFiltersAcl(Acl):
    """ACL for Location Filters resources."""

    _acl_name: ClassVar[str] = "locationFiltersAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | IDScope


class MonitoringTasksAcl(Acl):
    """ACL for Monitoring Tasks resources."""

    _acl_name: ClassVar[str] = "monitoringTasksAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class NotificationsAcl(Acl):
    """ACL for Notifications resources."""

    _acl_name: ClassVar[str] = "notificationsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class PipelinesAcl(Acl):
    """ACL for Pipelines resources."""

    _acl_name: ClassVar[str] = "pipelinesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class PostgresGatewayAcl(Acl):
    """ACL for PostgreSQL Gateway resources."""

    _acl_name: ClassVar[str] = "postgresGatewayAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | PostgresGatewayUsersScope


class ProjectsAcl(Acl):
    """ACL for Projects resources."""

    _acl_name: ClassVar[str] = "projectsAcl"
    actions: list[Literal["READ", "CREATE", "LIST", "UPDATE", "DELETE"]]
    scope: AllScope


class RawAcl(Acl):
    """ACL for RAW resources."""

    _acl_name: ClassVar[str] = "rawAcl"
    actions: list[Literal["READ", "WRITE", "LIST"]]
    scope: AllScope | TableScope


class RelationshipsAcl(Acl):
    """ACL for Relationships resources."""

    _acl_name: ClassVar[str] = "relationshipsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class RoboticsAcl(Acl):
    """ACL for Robotics resources."""

    _acl_name: ClassVar[str] = "roboticsAcl"
    actions: list[Literal["READ", "CREATE", "UPDATE", "DELETE"]]
    scope: AllScope | DataSetScope


class SAPWritebackAcl(Acl):
    """ACL for SAP Writeback resources."""

    _acl_name: ClassVar[str] = "sapWritebackAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | InstancesScope


class SAPWritebackRequestsAcl(Acl):
    """ACL for SAP Writeback Requests resources."""

    _acl_name: ClassVar[str] = "sapWritebackRequestsAcl"
    actions: list[Literal["WRITE", "LIST"]]
    scope: AllScope | InstancesScope


class ScheduledCalculationsAcl(Acl):
    """ACL for Scheduled Calculations resources."""

    _acl_name: ClassVar[str] = "scheduledCalculationsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class SecurityCategoriesAcl(Acl):
    """ACL for Security Categories resources."""

    _acl_name: ClassVar[str] = "securityCategoriesAcl"
    actions: list[Literal["MEMBEROF", "LIST", "CREATE", "UPDATE", "DELETE"]]
    scope: AllScope | IDScopeLowerCase


class SeismicAcl(Acl):
    """ACL for Seismic resources."""

    _acl_name: ClassVar[str] = "seismicAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | PartitionScope


class SequencesAcl(Acl):
    """ACL for Sequences resources."""

    _acl_name: ClassVar[str] = "sequencesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class SessionsAcl(Acl):
    """ACL for Sessions resources."""

    _acl_name: ClassVar[str] = "sessionsAcl"
    actions: list[Literal["LIST", "CREATE", "DELETE"]]
    scope: AllScope


class StreamRecordsAcl(Acl):
    """ACL for Stream Records resources."""

    _acl_name: ClassVar[str] = "streamRecordsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | SpaceIDScope


class StreamsAcl(Acl):
    """ACL for Streams resources."""

    _acl_name: ClassVar[str] = "streamsAcl"
    actions: list[Literal["READ", "CREATE", "DELETE"]]
    scope: AllScope


class TemplateGroupsAcl(Acl):
    """ACL for Template Groups resources."""

    _acl_name: ClassVar[str] = "templateGroupsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class TemplateInstancesAcl(Acl):
    """ACL for Template Instances resources."""

    _acl_name: ClassVar[str] = "templateInstancesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class ThreeDAcl(Acl):
    """ACL for 3D resources."""

    _acl_name: ClassVar[str] = "threedAcl"
    actions: list[Literal["READ", "CREATE", "UPDATE", "DELETE"]]
    scope: AllScope | DataSetScope


class TimeSeriesAcl(Acl):
    """ACL for Time Series resources."""

    _acl_name: ClassVar[str] = "timeSeriesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope | IDScopeLowerCase | AssetRootIDScope


class TimeSeriesSubscriptionsAcl(Acl):
    """ACL for Time Series Subscriptions resources."""

    _acl_name: ClassVar[str] = "timeSeriesSubscriptionsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class TransformationsAcl(Acl):
    """ACL for Transformations resources."""

    _acl_name: ClassVar[str] = "transformationsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class TypesAcl(Acl):
    """ACL for Types resources."""

    _acl_name: ClassVar[str] = "typesAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class UserProfilesAcl(Acl):
    """ACL for User Profiles resources."""

    _acl_name: ClassVar[str] = "userProfilesAcl"
    actions: list[Literal["READ"]]
    scope: AllScope


class VideoStreamingAcl(Acl):
    """ACL for Video Streaming resources."""

    _acl_name: ClassVar[str] = "videoStreamingAcl"
    actions: list[Literal["READ", "WRITE", "SUBSCRIBE", "PUBLISH"]]
    scope: AllScope | DataSetScope


class VisionModelAcl(Acl):
    """ACL for Vision Model resources."""

    _acl_name: ClassVar[str] = "visionModelAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class WellsAcl(Acl):
    """ACL for Wells resources."""

    _acl_name: ClassVar[str] = "wellsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope


class WorkflowOrchestrationAcl(Acl):
    """ACL for Workflow Orchestration resources."""

    _acl_name: ClassVar[str] = "workflowOrchestrationAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class SimulatorsAcl(Acl):
    """ACL for Simulators resources."""

    _acl_name: ClassVar[str] = "simulatorsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | DataSetScope


class UnknownAcl(Acl):
    """Fallback for unknown ACL types."""

    _acl_name: ClassVar[str] = "unknownAcl"
    actions: list[str]
    scope: AllScope


# Build ACL lookup from all Acl subclasses
def _get_all_acl_subclasses(cls: type) -> list[type]:
    """Get all subclasses of a class recursively."""
    subclasses = []
    for subclass in cls.__subclasses__():
        if subclass is not UnknownAcl:
            subclasses.append(subclass)
        subclasses.extend(_get_all_acl_subclasses(subclass))
    return subclasses


ACL_CLASS_BY_NAME: MappingProxyType[str, type[Acl]] = MappingProxyType(
    {cls._acl_name: cls for cls in _get_all_acl_subclasses(Acl) if hasattr(cls, "_acl_name")}
)


def parse_acl(acl_name: str, acl_data: dict[str, Any]) -> Acl:
    """Parse an ACL from its name and data."""
    if acl_name in ACL_CLASS_BY_NAME:
        return ACL_CLASS_BY_NAME[acl_name].model_validate(acl_data)
    # Unknown ACL type - return as UnknownAcl
    return UnknownAcl(actions=acl_data.get("actions", []), scope=AllScope())
