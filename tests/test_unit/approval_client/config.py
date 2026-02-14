from __future__ import annotations

from cognite.client.data_classes import (
    Asset,
    AssetList,
    AssetWrite,
    Database,
    DatabaseList,
    Datapoints,
    DatapointsList,
    DatapointSubscription,
    DatapointSubscriptionList,
    DataPointSubscriptionWrite,
    DataSet,
    DataSetList,
    DataSetWrite,
    Event,
    EventList,
    EventWrite,
    ExtractionPipeline,
    ExtractionPipelineConfig,
    ExtractionPipelineConfigWrite,
    ExtractionPipelineList,
    ExtractionPipelineWrite,
    FileMetadata,
    FileMetadataList,
    FileMetadataWrite,
    Function,
    FunctionList,
    FunctionSchedule,
    FunctionSchedulesList,
    FunctionScheduleWrite,
    FunctionWrite,
    Group,
    GroupList,
    GroupWrite,
    LabelDefinition,
    LabelDefinitionList,
    LabelDefinitionWrite,
    Relationship,
    RelationshipList,
    RelationshipWrite,
    Row,
    RowList,
    RowWrite,
    SecurityCategory,
    SecurityCategoryList,
    SecurityCategoryWrite,
    Sequence,
    SequenceList,
    SequenceRows,
    SequenceRowsList,
    SequenceWrite,
    Table,
    TableList,
    TableWrite,
    ThreeDModel,
    ThreeDModelList,
    ThreeDModelWrite,
    TimeSeries,
    TimeSeriesList,
    TimeSeriesWrite,
    Transformation,
    TransformationList,
    TransformationNotification,
    TransformationNotificationList,
    TransformationSchedule,
    TransformationScheduleList,
    TransformationScheduleWrite,
    TransformationWrite,
    Workflow,
    WorkflowList,
    WorkflowTrigger,
    WorkflowTriggerList,
    WorkflowTriggerUpsert,
    WorkflowUpsert,
    WorkflowVersion,
    WorkflowVersionList,
    WorkflowVersionUpsert,
)
from cognite.client.data_classes.agents import Agent, AgentList, AgentUpsert
from cognite.client.data_classes.data_modeling import (
    Container,
    ContainerApply,
    ContainerList,
    DataModel,
    DataModelApply,
    DataModelList,
    Node,
    NodeApply,
    NodeList,
    Space,
    SpaceApply,
    SpaceList,
    View,
    ViewApply,
    ViewList,
)
from cognite.client.data_classes.extractionpipelines import ExtractionPipelineConfigList
from cognite.client.data_classes.hosted_extractors import (
    Destination,
    DestinationList,
    DestinationWrite,
    Job,
    JobList,
    JobWrite,
    Mapping,
    MappingList,
    MappingWrite,
    Source,
    SourceList,
    SourceWrite,
)
from cognite.client.data_classes.iam import TokenInspection
from cognite.client.data_classes.transformations.notifications import (
    TransformationNotificationWrite,
)

from cognite_toolkit._cdf_tk.client.resource_classes.agent import AgentRequest, AgentResponse
from cognite_toolkit._cdf_tk.client.resource_classes.apm_config_v1 import (
    APMConfigRequest,
    APMConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.cognite_file import (
    CogniteFileRequest,
    CogniteFileResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerRequest,
    ContainerResponse,
    DataModelRequest,
    DataModelResponse,
    InstanceRequest,
    InstanceResponse,
    SpaceRequest,
    SpaceResponse,
    ViewRequest,
    ViewResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.dataset import DataSetRequest, DataSetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function import FunctionRequest, FunctionResponse
from cognite_toolkit._cdf_tk.client.resource_classes.function_schedule import (
    FunctionScheduleRequest,
    FunctionScheduleResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_destination import (
    HostedExtractorDestinationRequest,
    HostedExtractorDestinationResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_job import (
    HostedExtractorJobRequest,
    HostedExtractorJobResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_mapping import (
    HostedExtractorMappingRequest,
    HostedExtractorMappingResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.hosted_extractor_source import (
    HostedExtractorSourceRequestUnion,
    HostedExtractorSourceResponseUnion,
)
from cognite_toolkit._cdf_tk.client.resource_classes.infield import (
    InFieldCDMLocationConfigRequest,
    InFieldCDMLocationConfigResponse,
    InFieldLocationConfigRequest,
    InFieldLocationConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.label import LabelRequest, LabelResponse
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.graphql_data_models import (
    GraphQLDataModel,
    GraphQLDataModelList,
    GraphQLDataModelWrite,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.raw import RawDatabase
from cognite_toolkit._cdf_tk.client.resource_classes.location_filter import (
    LocationFilterRequest,
    LocationFilterResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.raw import (
    RAWDatabaseResponse,
    RAWTableResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.relationship import RelationshipRequest, RelationshipResponse
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import (
    ResourceViewMappingRequest,
    ResourceViewMappingResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.robotics import (
    RobotCapabilityRequest,
    RobotCapabilityResponse,
    RobotDataPostProcessingRequest,
    RobotDataPostProcessingResponse,
    RobotFrameRequest,
    RobotFrameResponse,
    RobotLocationRequest,
    RobotLocationResponse,
    RobotMapRequest,
    RobotMapResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.search_config import (
    SearchConfigResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.securitycategory import (
    SecurityCategoryRequest,
    SecurityCategoryResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.sequence import SequenceRequest, SequenceResponse
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model import (
    SimulatorModelRequest,
    SimulatorModelResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model_revision import (
    SimulatorModelRevisionRequest,
    SimulatorModelRevisionResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_routine import (
    SimulatorRoutineRequest,
    SimulatorRoutineResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_routine_revision import (
    SimulatorRoutineRevisionRequest,
    SimulatorRoutineRevisionResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamRequest, StreamResponse
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowRequest, WorkflowResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    WorkflowTriggerRequest,
    WorkflowTriggerResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    WorkflowVersionRequest,
    WorkflowVersionResponse,
)

from .data_classes import APIResource, Method

# This is used to define the resources that should be mocked in the ApprovalCogniteClient
# You can add more resources here if you need to mock more resources
API_RESOURCES = [
    APIResource(
        api_name="post",
        resource_cls=TokenInspection,
        methods={
            "post": [Method(api_class_method="post", mock_class_method="post_method")],
        },
    ),
    APIResource(
        api_name="iam.groups",
        resource_cls=Group,
        _write_cls=GroupWrite,
        _list_cls=GroupList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
            "retrieve": [Method(api_class_method="list", mock_class_method="return_values")],
        },
    ),
    APIResource(
        api_name="iam.token",
        resource_cls=TokenInspection,
        methods={
            "inspect": [Method(api_class_method="inspect", mock_class_method="return_value")],
        },
    ),
    APIResource(
        api_name="data_sets",
        resource_cls=DataSet,
        _write_cls=DataSetWrite,
        _list_cls=DataSetList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_value"),
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="time_series",
        resource_cls=TimeSeries,
        _write_cls=TimeSeriesWrite,
        _list_cls=TimeSeriesList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_values"),
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="raw.databases",
        resource_cls=Database,
        _write_cls=RawDatabase,
        _list_cls=DatabaseList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [Method(api_class_method="list", mock_class_method="return_values")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_raw")],
        },
    ),
    APIResource(
        api_name="raw.tables",
        resource_cls=Table,
        _write_cls=TableWrite,
        _list_cls=TableList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_raw_table")],
            "retrieve": [Method(api_class_method="list", mock_class_method="return_values")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_raw")],
        },
    ),
    APIResource(
        api_name="raw.rows",
        resource_cls=Row,
        _write_cls=RowWrite,
        _list_cls=RowList,
        methods={
            "create": [Method(api_class_method="insert_dataframe", mock_class_method="insert_dataframe")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_raw")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="functions",
        resource_cls=Function,
        _write_cls=FunctionWrite,
        _list_cls=FunctionList,
        methods={
            "create": [
                Method(api_class_method="create", mock_class_method="create_single"),
            ],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_value"),
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="functions.schedules",
        resource_cls=FunctionSchedule,
        _write_cls=FunctionScheduleWrite,
        _list_cls=FunctionSchedulesList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_single")],
        },
    ),
    APIResource(
        api_name="transformations",
        resource_cls=Transformation,
        _write_cls=TransformationWrite,
        _list_cls=TransformationList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_value"),
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="transformations.schedules",
        resource_cls=TransformationSchedule,
        _write_cls=TransformationScheduleWrite,
        _list_cls=TransformationScheduleList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_value"),
            ],
        },
    ),
    APIResource(
        api_name="extraction_pipelines",
        resource_cls=ExtractionPipeline,
        _write_cls=ExtractionPipelineWrite,
        _list_cls=ExtractionPipelineList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_value"),
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="extraction_pipelines.config",
        resource_cls=ExtractionPipelineConfig,
        _write_cls=ExtractionPipelineConfigWrite,
        _list_cls=ExtractionPipelineConfigList,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_extraction_pipeline_config")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_value"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.containers",
        resource_cls=Container,
        _list_cls=ContainerList,
        _write_cls=ContainerApply,
        methods={
            "create": [Method(api_class_method="apply", mock_class_method="create_multiple")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_data_modeling")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="data_model_retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.views",
        resource_cls=View,
        _list_cls=ViewList,
        _write_cls=ViewApply,
        methods={
            "create": [Method(api_class_method="apply", mock_class_method="create_multiple")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_data_modeling")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="data_model_retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.data_models",
        resource_cls=DataModel,
        _list_cls=DataModelList,
        _write_cls=DataModelApply,
        methods={
            "create": [Method(api_class_method="apply", mock_class_method="create_multiple")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_data_modeling")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_data_models"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.spaces",
        resource_cls=Space,
        _list_cls=SpaceList,
        _write_cls=SpaceApply,
        methods={
            "create": [Method(api_class_method="apply", mock_class_method="create_multiple")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_space")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="time_series.data",
        resource_cls=Datapoints,
        _list_cls=DatapointsList,
        methods={
            "create": [
                Method(api_class_method="insert", mock_class_method="create_multiple"),
                Method(api_class_method="insert_dataframe", mock_class_method="insert_dataframe"),
            ],
        },
    ),
    APIResource(
        api_name="files",
        resource_cls=FileMetadata,
        _list_cls=FileMetadataList,
        _write_cls=FileMetadataWrite,
        methods={
            "create": [
                Method(api_class_method="upload", mock_class_method="upload"),
                Method(api_class_method="create", mock_class_method="create_filemetadata"),
                # This is used by functions to upload the file used for deployment.
                Method(api_class_method="upload_bytes", mock_class_method="upload_bytes_files_api"),
                Method(
                    api_class_method="upload_content_bytes", mock_class_method="upload_file_content_bytes_files_api"
                ),
                Method(api_class_method="upload_content", mock_class_method="upload_file_content_path_files_api"),
            ],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="files_retrieve"),
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="data_modeling.instances",
        resource_cls=Node,
        _list_cls=NodeList,
        _write_cls=NodeApply,
        methods={
            "create": [Method(api_class_method="apply", mock_class_method="create_instances")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_instances")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_instances"),
            ],
        },
    ),
    APIResource(
        api_name="workflows",
        resource_cls=Workflow,
        _list_cls=WorkflowList,
        _write_cls=WorkflowUpsert,
        methods={
            "create": [Method(api_class_method="upsert", mock_class_method="upsert")],
            # "update": [Method(api_class_method="upsert", mock_name="upsert")],
            # "delete": [Method(api_class_method="delete", mock_name="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="workflows.versions",
        resource_cls=WorkflowVersion,
        _list_cls=WorkflowVersionList,
        _write_cls=WorkflowVersionUpsert,
        methods={
            "create": [Method(api_class_method="upsert", mock_class_method="upsert")],
            # "update": [Method(api_class_method="upsert", mock_name="upsert")],
            # "delete": [Method(api_class_method="delete", mock_name="delete")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
                Method(api_class_method="list", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="iam.security_categories",
        resource_cls=SecurityCategory,
        _list_cls=SecurityCategoryList,
        _write_cls=SecurityCategoryWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [Method(api_class_method="list", mock_class_method="return_values")],
        },
    ),
    APIResource(
        api_name="time_series.subscriptions",
        resource_cls=DatapointSubscription,
        _list_cls=DatapointSubscriptionList,
        _write_cls=DataPointSubscriptionWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_single")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_value"),
            ],
        },
    ),
    APIResource(
        api_name="labels",
        resource_cls=LabelDefinition,
        _list_cls=LabelDefinitionList,
        _write_cls=LabelDefinitionWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [Method(api_class_method="list", mock_class_method="return_values")],
        },
    ),
    APIResource(
        api_name="transformations.notifications",
        resource_cls=TransformationNotification,
        _list_cls=TransformationNotificationList,
        _write_cls=TransformationNotificationWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [Method(api_class_method="list", mock_class_method="return_values")],
        },
    ),
    APIResource(
        api_name="assets",
        resource_cls=Asset,
        _list_cls=AssetList,
        _write_cls=AssetWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
        },
    ),
    APIResource(
        api_name="three_d.models",
        resource_cls=ThreeDModel,
        _list_cls=ThreeDModelList,
        _write_cls=ThreeDModelWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_3dmodel")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="__iter__", mock_class_method="iterate_values"),
            ],
        },
    ),
    APIResource(
        api_name="dml",
        resource_cls=GraphQLDataModel,
        _list_cls=GraphQLDataModelList,
        _write_cls=GraphQLDataModelWrite,
        methods={
            "create": [Method(api_class_method="apply_dml", mock_class_method="apply_dml")],
        },
    ),
    APIResource(
        api_name="sequences",
        resource_cls=Sequence,
        _list_cls=SequenceList,
        _write_cls=SequenceWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="sequences.rows",
        resource_cls=SequenceRows,
        _list_cls=SequenceRowsList,
        _write_cls=SequenceRows,
        methods={
            "create": [Method(api_class_method="insert", mock_class_method="create_single")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="workflows.triggers",
        resource_cls=WorkflowTrigger,
        _list_cls=WorkflowTriggerList,
        _write_cls=WorkflowTriggerUpsert,
        methods={
            "create": [Method(api_class_method="upsert", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="hosted_extractors.sources",
        resource_cls=Source,
        _list_cls=SourceList,
        _write_cls=SourceWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="hosted_extractors.destinations",
        resource_cls=Destination,
        _list_cls=DestinationList,
        _write_cls=DestinationWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="hosted_extractors.jobs",
        resource_cls=Job,
        _list_cls=JobList,
        _write_cls=JobWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="hosted_extractors.mappings",
        resource_cls=Mapping,
        _list_cls=MappingList,
        _write_cls=MappingWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="events",
        resource_cls=Event,
        _list_cls=EventList,
        _write_cls=EventWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="agents",
        resource_cls=Agent,
        _list_cls=AgentList,
        _write_cls=AgentUpsert,
        methods={
            "create": [Method(api_class_method="upsert", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
            "delete": [
                Method(api_class_method="delete", mock_class_method="delete_id_external_id"),
            ],
        },
    ),
    APIResource(
        api_name="relationships",
        resource_cls=Relationship,
        _list_cls=RelationshipList,
        _write_cls=RelationshipWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve_multiple", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="migration.resource_view_mapping",
        resource_cls=ResourceViewMappingResponse,
        _write_cls=ResourceViewMappingRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="infield.config",
        resource_cls=InFieldLocationConfigResponse,
        _write_cls=InFieldLocationConfigRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="infield.cdm_config",
        resource_cls=InFieldCDMLocationConfigResponse,
        _write_cls=InFieldCDMLocationConfigRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="infield.apm_config",
        resource_cls=APMConfigResponse,
        _write_cls=APMConfigRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.cognite_files",
        resource_cls=CogniteFileResponse,
        _write_cls=CogniteFileRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="streams",
        resource_cls=StreamResponse,
        _write_cls=StreamRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.security_categories",
        resource_cls=SecurityCategoryResponse,
        _write_cls=SecurityCategoryRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="list"),
            ],
        },
    ),
    APIResource(
        api_name="tool.labels",
        resource_cls=LabelResponse,
        _write_cls=LabelRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.assets",
        resource_cls=AssetResponse,
        _write_cls=AssetRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.events",
        resource_cls=EventResponse,
        _write_cls=EventRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.filemetadata",
        resource_cls=FileMetadataResponse,
        _write_cls=FileMetadataRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.relationships",
        resource_cls=RelationshipResponse,
        _write_cls=RelationshipRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.timeseries",
        resource_cls=TimeSeriesResponse,
        _write_cls=TimeSeriesRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.sequences",
        resource_cls=SequenceResponse,
        _write_cls=SequenceRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.spaces",
        resource_cls=SpaceResponse,
        _write_cls=SpaceRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.containers",
        resource_cls=ContainerResponse,
        _write_cls=ContainerRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.views",
        resource_cls=ViewResponse,
        _write_cls=ViewRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.data_models",
        resource_cls=DataModelResponse,
        _write_cls=DataModelRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
                Method(api_class_method="list", mock_class_method="list"),
            ],
        },
    ),
    APIResource(
        api_name="tool.instances",
        resource_cls=InstanceResponse,
        _write_cls=InstanceRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_instances_pydantic")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve_instances_pydantic"),
            ],
        },
    ),
    APIResource(
        api_name="tool.simulators.models",
        resource_cls=SimulatorModelResponse,
        _write_cls=SimulatorModelRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.simulators.model_revisions",
        resource_cls=SimulatorModelRevisionResponse,
        _write_cls=SimulatorModelRevisionRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.simulators.routines",
        resource_cls=SimulatorRoutineResponse,
        _write_cls=SimulatorRoutineRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.simulators.routine_revisions",
        resource_cls=SimulatorRoutineRevisionResponse,
        _write_cls=SimulatorRoutineRevisionRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.hosted_extractors.sources",
        resource_cls=HostedExtractorSourceRequestUnion,
        _write_cls=HostedExtractorSourceResponseUnion,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_hosted_extractor_source")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve_hosted_extractor_source"),
            ],
        },
    ),
    APIResource(
        api_name="tool.hosted_extractors.jobs",
        resource_cls=HostedExtractorJobRequest,
        _write_cls=HostedExtractorJobResponse,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.hosted_extractors.destinations",
        resource_cls=HostedExtractorDestinationRequest,
        _write_cls=HostedExtractorDestinationResponse,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.hosted_extractors.mappings",
        resource_cls=HostedExtractorMappingRequest,
        _write_cls=HostedExtractorMappingResponse,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.robotics.capabilities",
        resource_cls=RobotCapabilityResponse,
        _write_cls=RobotCapabilityRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.robotics.locations",
        resource_cls=RobotLocationResponse,
        _write_cls=RobotLocationRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.robotics.frames",
        resource_cls=RobotFrameResponse,
        _write_cls=RobotFrameRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.robotics.data_postprocessing",
        resource_cls=RobotDataPostProcessingResponse,
        _write_cls=RobotDataPostProcessingRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.robotics.maps",
        resource_cls=RobotMapResponse,
        _write_cls=RobotMapRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.location_filters",
        resource_cls=LocationFilterResponse,
        _write_cls=LocationFilterRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.raw.databases",
        resource_cls=RAWDatabaseResponse,
        _write_cls=RAWDatabaseResponse,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="list_raw_db"),
            ],
        },
    ),
    APIResource(
        api_name="tool.raw.tables",
        resource_cls=RAWTableResponse,
        _write_cls=RAWTableResponse,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="list_raw_table"),
            ],
        },
    ),
    APIResource(
        api_name="tool.workflows",
        resource_cls=WorkflowResponse,
        _write_cls=WorkflowRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.workflows.versions",
        resource_cls=WorkflowVersionResponse,
        _write_cls=WorkflowVersionRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.workflows.triggers",
        resource_cls=WorkflowTriggerResponse,
        _write_cls=WorkflowTriggerRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="list"),
            ],
        },
    ),
    APIResource(
        api_name="tool.search_configurations",
        resource_cls=SearchConfigResponse,
        _write_cls=SearchConfigResponse,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="list"),
            ],
        },
    ),
    APIResource(
        api_name="tool.functions",
        resource_cls=FunctionResponse,
        _write_cls=FunctionRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
            ],
        },
    ),
    APIResource(
        api_name="tool.functions.schedules",
        resource_cls=FunctionScheduleResponse,
        _write_cls=FunctionScheduleRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="list"),
            ],
        },
    ),
    APIResource(
        api_name="tool.agents",
        resource_cls=AgentResponse,
        _write_cls=AgentRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [Method(api_class_method="retrieve", mock_class_method="retrieve")],
        },
    ),
    APIResource(
        api_name="tool.datasets",
        resource_cls=DataSetResponse,
        _write_cls=DataSetRequest,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="retrieve"),
                Method(api_class_method="iterate", mock_class_method="iterate"),
                Method(api_class_method="list", mock_class_method="list"),
            ],
        },
    ),
]
