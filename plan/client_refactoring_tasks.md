# Refactoring: Internal ResourceAPI Classes

The goal of this refactoring is to create internal ResourceAPI classes for all resources that Toolkit uses instead of relying on the external ResourceAPI from the `cognite-sdk` package.

For each resource, the following tasks need to be completed:
1. Create `pydantic` request and response objects (in `cognite_toolkit/_cdf_tk/client/data_classes/`)
2. Create a `CDFResourceAPI` for the resource (in `cognite_toolkit/_cdf_tk/client/api/`)
3. Register the new API under `.tool` in `ToolkitClient`
4. Replace client calls in the `ResourceCRUD` implementation

---

## Auth Resources

### GroupCRUD
- [ ] Create `GroupRequest` and `GroupResponse` pydantic models (CDF-26628)
- [ ] Create `GroupsAPI` class and register in `ToolAPI` (CDF-26629)
- [ ] Update `GroupCRUD` to use new API (CDF-26630)

### SecurityCategoryCRUD
- [ ] Create `SecurityCategoryRequest` and `SecurityCategoryResponse` pydantic models (CDF-26631)
- [ ] Create `SecurityCategoriesAPI` class and register in `ToolAPI`
- [ ] Update `SecurityCategoryCRUD` to use new API

---

## Classic Resources

### AssetCRUD ✅ DONE
- [x] Create `AssetRequest` and `AssetResponse` pydantic models
- [x] Create `AssetsAPI` class and register in `ToolAPI`
- [x] Update `AssetCRUD` to use new API

### SequenceCRUD
- [ ] Create `SequenceRequest` and `SequenceResponse` pydantic models
- [ ] Create `SequencesAPI` class and register in `ToolAPI`
- [ ] Update `SequenceCRUD` to use new API

### SequenceRowCRUD
- [ ] Create `SequenceRowsRequest` and `SequenceRowsResponse` pydantic models
- [ ] Create `SequenceRowsAPI` class and register in `ToolAPI`
- [ ] Update `SequenceRowCRUD` to use new API

### EventCRUD
- [x] Create `EventRequest` and `EventResponse` pydantic models
- [ ] Create `EventsAPI` class and register in `ToolAPI`
- [ ] Update `EventCRUD` to use new API

---

## Configuration Resources

### SearchConfigCRUD
- [ ] Create `SearchConfigRequest` and `SearchConfigResponse` pydantic models
- [ ] Create `SearchConfigAPI` class and register in `ToolAPI`
- [ ] Update `SearchConfigCRUD` to use new API

---

## Data Organization Resources

### DataSetsCRUD
- [ ] Create `DataSetRequest` and `DataSetResponse` pydantic models
- [ ] Create `DataSetsAPI` class and register in `ToolAPI`
- [ ] Update `DataSetsCRUD` to use new API

### LabelCRUD
- [ ] Create `LabelRequest` and `LabelResponse` pydantic models
- [ ] Create `LabelsAPI` class and register in `ToolAPI`
- [ ] Update `LabelCRUD` to use new API

---

## Data Modeling Resources

### SpaceCRUD
- [ ] Create `SpaceRequest` and `SpaceResponse` pydantic models
- [ ] Create `SpacesAPI` class and register in `ToolAPI`
- [ ] Update `SpaceCRUD` to use new API

### ContainerCRUD
- [ ] Create `ContainerRequest` and `ContainerResponse` pydantic models
- [ ] Create `ContainersAPI` class and register in `ToolAPI`
- [ ] Update `ContainerCRUD` to use new API

### ViewCRUD
- [ ] Create `ViewRequest` and `ViewResponse` pydantic models
- [ ] Create `ViewsAPI` class and register in `ToolAPI`
- [ ] Update `ViewCRUD` to use new API

### DataModelCRUD
- [ ] Create `DataModelRequest` and `DataModelResponse` pydantic models
- [ ] Create `DataModelsAPI` class and register in `ToolAPI`
- [ ] Update `DataModelCRUD` to use new API

### NodeCRUD
- [ ] Create `NodeRequest` and `NodeResponse` pydantic models
- [ ] Create `NodesAPI` class and register in `ToolAPI`
- [ ] Update `NodeCRUD` to use new API

### EdgeCRUD
- [ ] Create `EdgeRequest` and `EdgeResponse` pydantic models
- [ ] Create `EdgesAPI` class and register in `ToolAPI`
- [ ] Update `EdgeCRUD` to use new API

### GraphQLCRUD
- [ ] Create `GraphQLDataModelRequest` and `GraphQLDataModelResponse` pydantic models
- [ ] Create `GraphQLAPI` class and register in `ToolAPI`
- [ ] Update `GraphQLCRUD` to use new API

---

## Extraction Pipeline Resources

### ExtractionPipelineCRUD
- [ ] Create `ExtractionPipelineRequest` and `ExtractionPipelineResponse` pydantic models
- [ ] Create `ExtractionPipelinesAPI` class and register in `ToolAPI`
- [ ] Update `ExtractionPipelineCRUD` to use new API

---

## Field Ops Resources

### InfieldV1CRUD
- [ ] Create `APMConfigRequest` and `APMConfigResponse` pydantic models
- [ ] Create `InfieldAPMConfigAPI` class and register in `ToolAPI`
- [ ] Update `InfieldV1CRUD` to use new API

### InFieldLocationConfigCRUD
- [ ] Create `InfieldLocationConfigRequest` and `InfieldLocationConfigResponse` pydantic models
- [ ] Create `InfieldLocationConfigAPI` class and register in `ToolAPI`
- [ ] Update `InFieldLocationConfigCRUD` to use new API

---

## File Resources

### FileMetadataCRUD
- [ ] Create `FileMetadataRequest` and `FileMetadataResponse` pydantic models
- [ ] Create `FilesAPI` class and register in `ToolAPI`
- [ ] Update `FileMetadataCRUD` to use new API

### CogniteFileCRUD
- [ ] Create `CogniteFileRequest` and `CogniteFileResponse` pydantic models
- [ ] Create `CogniteFilesAPI` class and register in `ToolAPI`
- [ ] Update `CogniteFileCRUD` to use new API

---

## Function Resources

### FunctionCRUD
- [ ] Create `FunctionRequest` and `FunctionResponse` pydantic models
- [ ] Create `FunctionsAPI` class and register in `ToolAPI`
- [ ] Update `FunctionCRUD` to use new API

### FunctionScheduleCRUD
- [ ] Create `FunctionScheduleRequest` and `FunctionScheduleResponse` pydantic models
- [ ] Create `FunctionSchedulesAPI` class and register in `ToolAPI`
- [ ] Update `FunctionScheduleCRUD` to use new API

---

## Hosted Extractors Resources

### HostedExtractorSourceCRUD
- [ ] Create `HostedExtractorSourceRequest` and `HostedExtractorSourceResponse` pydantic models
- [ ] Create `HostedExtractorSourcesAPI` class and register in `ToolAPI`
- [ ] Update `HostedExtractorSourceCRUD` to use new API

### HostedExtractorDestinationCRUD
- [ ] Create `HostedExtractorDestinationRequest` and `HostedExtractorDestinationResponse` pydantic models
- [ ] Create `HostedExtractorDestinationsAPI` class and register in `ToolAPI`
- [ ] Update `HostedExtractorDestinationCRUD` to use new API

### HostedExtractorJobCRUD
- [ ] Create `HostedExtractorJobRequest` and `HostedExtractorJobResponse` pydantic models
- [ ] Create `HostedExtractorJobsAPI` class and register in `ToolAPI`
- [ ] Update `HostedExtractorJobCRUD` to use new API

### HostedExtractorMappingCRUD
- [ ] Create `HostedExtractorMappingRequest` and `HostedExtractorMappingResponse` pydantic models
- [ ] Create `HostedExtractorMappingsAPI` class and register in `ToolAPI`
- [ ] Update `HostedExtractorMappingCRUD` to use new API

---

## Industrial Tool Resources

### StreamlitCRUD
- [ ] Create `StreamlitRequest` and `StreamlitResponse` pydantic models
- [ ] Create `StreamlitAPI` class and register in `ToolAPI`
- [ ] Update `StreamlitCRUD` to use new API

---

## Location Resources

### LocationFilterCRUD
- [ ] Create `LocationFilterRequest` and `LocationFilterResponse` pydantic models
- [ ] Create `LocationFiltersAPI` class and register in `ToolAPI`
- [ ] Update `LocationFilterCRUD` to use new API

---

## Migration Resources

### ResourceViewMappingCRUD
- [ ] Create `ResourceViewMappingRequest` and `ResourceViewMappingResponse` pydantic models
- [ ] Create `ResourceViewMappingsAPI` class and register in `ToolAPI`
- [ ] Update `ResourceViewMappingCRUD` to use new API

---

## Raw Resources

### RawDatabaseCRUD
- [ ] Create `RawDatabaseRequest` and `RawDatabaseResponse` pydantic models
- [ ] Create `RawDatabasesAPI` class and register in `ToolAPI`
- [ ] Update `RawDatabaseCRUD` to use new API

### RawTableCRUD
- [ ] Create `RawTableRequest` and `RawTableResponse` pydantic models
- [ ] Create `RawTablesAPI` class and register in `ToolAPI`
- [ ] Update `RawTableCRUD` to use new API

---

## Relationship Resources

### RelationshipCRUD
- [ ] Create `RelationshipRequest` and `RelationshipResponse` pydantic models
- [ ] Create `RelationshipsAPI` class and register in `ToolAPI`
- [ ] Update `RelationshipCRUD` to use new API

---

## Robotics Resources

### RoboticFrameCRUD
- [ ] Create `RoboticFrameRequest` and `RoboticFrameResponse` pydantic models
- [ ] Create `RoboticFramesAPI` class and register in `ToolAPI`
- [ ] Update `RoboticFrameCRUD` to use new API

### RoboticLocationCRUD
- [ ] Create `RoboticLocationRequest` and `RoboticLocationResponse` pydantic models
- [ ] Create `RoboticLocationsAPI` class and register in `ToolAPI`
- [ ] Update `RoboticLocationCRUD` to use new API

### RoboticsDataPostProcessingCRUD
- [ ] Create `DataPostProcessingRequest` and `DataPostProcessingResponse` pydantic models
- [ ] Create `DataPostProcessingAPI` class and register in `ToolAPI`
- [ ] Update `RoboticsDataPostProcessingCRUD` to use new API

### RobotCapabilityCRUD
- [ ] Create `RobotCapabilityRequest` and `RobotCapabilityResponse` pydantic models
- [ ] Create `RobotCapabilitiesAPI` class and register in `ToolAPI`
- [ ] Update `RobotCapabilityCRUD` to use new API

### RoboticMapCRUD
- [ ] Create `RoboticMapRequest` and `RoboticMapResponse` pydantic models
- [ ] Create `RoboticMapsAPI` class and register in `ToolAPI`
- [ ] Update `RoboticMapCRUD` to use new API

---

## Streams Resources

### StreamCRUD
- [ ] Create `StreamRequest` and `StreamResponse` pydantic models
- [ ] Create `StreamsAPI` class and register in `ToolAPI`
- [ ] Update `StreamCRUD` to use new API

---

## Three D Model Resources

### ThreeDModelCRUD
- [ ] Create `ThreeDModelRequest` and `ThreeDModelResponse` pydantic models
- [ ] Create `ThreeDModelsAPI` class and register in `ToolAPI`
- [ ] Update `ThreeDModelCRUD` to use new API

---

## Timeseries Resources

### TimeSeriesCRUD
- [x] Create `TimeSeriesRequest` and `TimeSeriesResponse` pydantic models
- [ ] Create `TimeSeriesAPI` class and register in `ToolAPI`
- [ ] Update `TimeSeriesCRUD` to use new API

---

## Transformation Resources

### TransformationCRUD
- [ ] Create `TransformationRequest` and `TransformationResponse` pydantic models
- [ ] Create `TransformationsAPI` class and register in `ToolAPI`
- [ ] Update `TransformationCRUD` to use new API

---

## Workflow Resources

### WorkflowCRUD
- [ ] Create `WorkflowRequest` and `WorkflowResponse` pydantic models
- [ ] Create `WorkflowsAPI` class and register in `ToolAPI`
- [ ] Update `WorkflowCRUD` to use new API

### WorkflowVersionCRUD
- [ ] Create `WorkflowVersionRequest` and `WorkflowVersionResponse` pydantic models
- [ ] Create `WorkflowVersionsAPI` class and register in `ToolAPI`
- [ ] Update `WorkflowVersionCRUD` to use new API

### WorkflowTriggerCRUD
- [ ] Create `WorkflowTriggerRequest` and `WorkflowTriggerResponse` pydantic models
- [ ] Create `WorkflowTriggersAPI` class and register in `ToolAPI`
- [ ] Update `WorkflowTriggerCRUD` to use new API

---

## Agent Resources

### AgentCRUD
- [ ] Create `AgentRequest` and `AgentResponse` pydantic models
- [ ] Create `AgentsAPI` class and register in `ToolAPI`
- [ ] Update `AgentCRUD` to use new API

