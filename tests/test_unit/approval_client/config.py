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

from cognite_toolkit._cdf_tk.client.resource_classes.asset import AssetRequest, AssetResponse
from cognite_toolkit._cdf_tk.client.resource_classes.event import EventRequest, EventResponse
from cognite_toolkit._cdf_tk.client.resource_classes.filemetadata import FileMetadataRequest, FileMetadataResponse
from cognite_toolkit._cdf_tk.client.resource_classes.infield import (
    InFieldCDMLocationConfig,
    InfieldLocationConfig,
    InfieldLocationConfigList,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy import robotics
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.graphql_data_models import (
    GraphQLDataModel,
    GraphQLDataModelList,
    GraphQLDataModelWrite,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.location_filters import (
    LocationFilter,
    LocationFilterList,
    LocationFilterWrite,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.migration import (
    ResourceViewMapping,
    ResourceViewMappingApply,
)
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.raw import RawDatabase
from cognite_toolkit._cdf_tk.client.resource_classes.legacy.search_config import (
    SearchConfig,
    SearchConfigList,
    SearchConfigWrite,
)
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model import (
    SimulatorModelRequest,
    SimulatorModelResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.streams import StreamRequest, StreamResponse
from cognite_toolkit._cdf_tk.client.resource_classes.timeseries import TimeSeriesRequest, TimeSeriesResponse

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
                Method(api_class_method="create_with_429_retry", mock_class_method="create_single"),
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
        api_name="robotics.data_postprocessing",
        resource_cls=robotics.DataPostProcessing,
        _list_cls=robotics.DataPostProcessingList,
        _write_cls=robotics.DataPostProcessingWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="robotics.frames",
        resource_cls=robotics.Frame,
        _list_cls=robotics.FrameList,
        _write_cls=robotics.FrameWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="robotics.locations",
        resource_cls=robotics.Location,
        _list_cls=robotics.LocationList,
        _write_cls=robotics.LocationWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="robotics.maps",
        resource_cls=robotics.Map,
        _list_cls=robotics.MapList,
        _write_cls=robotics.MapWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="robotics.capabilities",
        resource_cls=robotics.RobotCapability,
        _list_cls=robotics.RobotCapabilityList,
        _write_cls=robotics.RobotCapabilityWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="search.locations",
        resource_cls=LocationFilter,
        _list_cls=LocationFilterList,
        _write_cls=LocationFilterWrite,
        methods={
            "create": [Method(api_class_method="create", mock_class_method="create_single")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
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
        resource_cls=ResourceViewMapping,
        _list_cls=NodeList[ResourceViewMapping],
        _write_cls=ResourceViewMappingApply,
        methods={
            "create": [Method(api_class_method="upsert", mock_class_method="create_nodes")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_id_external_id")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="search.configurations",
        resource_cls=SearchConfig,
        _list_cls=SearchConfigList,
        _write_cls=SearchConfigWrite,
        methods={
            "create": [Method(api_class_method="upsert", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="infield.config",
        resource_cls=InfieldLocationConfig,
        _list_cls=InfieldLocationConfigList,
        _write_cls=InfieldLocationConfig,
        methods={
            "create": [Method(api_class_method="apply", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
    APIResource(
        api_name="infield.cdm_config",
        resource_cls=InFieldCDMLocationConfig,
        _write_cls=InFieldCDMLocationConfig,
        # Todo: Remove these.
        _list_cls=InfieldLocationConfigList,
        methods={
            "create": [Method(api_class_method="apply", mock_class_method="create_multiple")],
            "retrieve": [
                Method(api_class_method="retrieve", mock_class_method="return_values"),
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
]
