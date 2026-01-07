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
- [ ] Create `SecurityCategoriesAPI` class and register in `ToolAPI` (CDF-26632)
- [ ] Update `SecurityCategoryCRUD` to use new API (CDF-26633)

---

## Classic Resources

### AssetCRUD ✅ DONE
- [x] Create `AssetRequest` and `AssetResponse` pydantic models
- [x] Create `AssetsAPI` class and register in `ToolAPI`
- [x] Update `AssetCRUD` to use new API

### SequenceCRUD
- [ ] Create `SequenceRequest` and `SequenceResponse` pydantic models (CDF-26634)
- [ ] Create `SequencesAPI` class and register in `ToolAPI` (CDF-26635)
- [ ] Update `SequenceCRUD` to use new API (CDF-26636)

### SequenceRowCRUD
- [ ] Create `SequenceRowsRequest` and `SequenceRowsResponse` pydantic models (CDF-26637)
- [ ] Create `SequenceRowsAPI` class and register in `ToolAPI` (CDF-26638)
- [ ] Update `SequenceRowCRUD` to use new API (CDF-26639)

### EventCRUD
- [x] Create `EventRequest` and `EventResponse` pydantic models
- [ ] Create `EventsAPI` class and register in `ToolAPI` (CDF-26640)
- [ ] Update `EventCRUD` to use new API (CDF-26641)

---

## Configuration Resources

### SearchConfigCRUD
- [ ] Create `SearchConfigRequest` and `SearchConfigResponse` pydantic models (CDF-26642)
- [ ] Create `SearchConfigAPI` class and register in `ToolAPI` (CDF-26643)
- [ ] Update `SearchConfigCRUD` to use new API (CDF-26644)

---

## Data Organization Resources

### DataSetsCRUD
- [ ] Create `DataSetRequest` and `DataSetResponse` pydantic models (CDF-26645)
- [ ] Create `DataSetsAPI` class and register in `ToolAPI` (CDF-26646)
- [ ] Update `DataSetsCRUD` to use new API (CDF-26647)

### LabelCRUD
- [ ] Create `LabelRequest` and `LabelResponse` pydantic models (CDF-26648)
- [ ] Create `LabelsAPI` class and register in `ToolAPI` (CDF-26649)
- [ ] Update `LabelCRUD` to use new API (CDF-26650)

---

## Data Modeling Resources

### SpaceCRUD
- [ ] Create `SpaceRequest` and `SpaceResponse` pydantic models (CDF-26651)
- [ ] Create `SpacesAPI` class and register in `ToolAPI` (CDF-26652)
- [ ] Update `SpaceCRUD` to use new API (CDF-26653)

### ContainerCRUD
- [ ] Create `ContainerRequest` and `ContainerResponse` pydantic models (CDF-26654)
- [ ] Create `ContainersAPI` class and register in `ToolAPI` (CDF-26655)
- [ ] Update `ContainerCRUD` to use new API (CDF-26656)

### ViewCRUD
- [ ] Create `ViewRequest` and `ViewResponse` pydantic models (CDF-26657)
- [ ] Create `ViewsAPI` class and register in `ToolAPI` (CDF-26658)
- [ ] Update `ViewCRUD` to use new API (CDF-26659)

### DataModelCRUD
- [ ] Create `DataModelRequest` and `DataModelResponse` pydantic models (CDF-26660)
- [ ] Create `DataModelsAPI` class and register in `ToolAPI` (CDF-26661)
- [ ] Update `DataModelCRUD` to use new API (CDF-26662)

### NodeCRUD
- [ ] Create `NodeRequest` and `NodeResponse` pydantic models (CDF-26663)
- [ ] Create `NodesAPI` class and register in `ToolAPI` (CDF-26664)
- [ ] Update `NodeCRUD` to use new API (CDF-26665)

### EdgeCRUD
- [ ] Create `EdgeRequest` and `EdgeResponse` pydantic models (CDF-26666)
- [ ] Create `EdgesAPI` class and register in `ToolAPI` (CDF-26667)
- [ ] Update `EdgeCRUD` to use new API (CDF-26668)

### GraphQLCRUD
- [ ] Create `GraphQLDataModelRequest` and `GraphQLDataModelResponse` pydantic models (CDF-26669)
- [ ] Create `GraphQLAPI` class and register in `ToolAPI` (CDF-26670)
- [ ] Update `GraphQLCRUD` to use new API (CDF-26671)

---

## Extraction Pipeline Resources

### ExtractionPipelineCRUD
- [ ] Create `ExtractionPipelineRequest` and `ExtractionPipelineResponse` pydantic models (CDF-26672)
- [ ] Create `ExtractionPipelinesAPI` class and register in `ToolAPI` (CDF-26673)
- [ ] Update `ExtractionPipelineCRUD` to use new API (CDF-26674)

---

## Field Ops Resources

### InfieldV1CRUD
- [ ] Create `APMConfigRequest` and `APMConfigResponse` pydantic models (CDF-26675)
- [ ] Create `InfieldAPMConfigAPI` class and register in `ToolAPI` (CDF-26676)
- [ ] Update `InfieldV1CRUD` to use new API (CDF-26677)

### InFieldLocationConfigCRUD
- [ ] Create `InfieldLocationConfigRequest` and `InfieldLocationConfigResponse` pydantic models (CDF-26678)
- [ ] Create `InfieldLocationConfigAPI` class and register in `ToolAPI` (CDF-26679)
- [ ] Update `InFieldLocationConfigCRUD` to use new API (CDF-26680)

---

## File Resources

### FileMetadataCRUD
- [ ] Create `FileMetadataRequest` and `FileMetadataResponse` pydantic models (CDF-26681)
- [ ] Create `FilesAPI` class and register in `ToolAPI` (CDF-26682)
- [ ] Update `FileMetadataCRUD` to use new API (CDF-26683)

### CogniteFileCRUD
- [ ] Create `CogniteFileRequest` and `CogniteFileResponse` pydantic models (CDF-26684)
- [ ] Create `CogniteFilesAPI` class and register in `ToolAPI` (CDF-26685)
- [ ] Update `CogniteFileCRUD` to use new API (CDF-26686)

---

## Function Resources

### FunctionCRUD
- [ ] Create `FunctionRequest` and `FunctionResponse` pydantic models (CDF-26687)
- [ ] Create `FunctionsAPI` class and register in `ToolAPI` (CDF-26688)
- [ ] Update `FunctionCRUD` to use new API (CDF-26689)

### FunctionScheduleCRUD
- [ ] Create `FunctionScheduleRequest` and `FunctionScheduleResponse` pydantic models (CDF-26690)
- [ ] Create `FunctionSchedulesAPI` class and register in `ToolAPI` (CDF-26691)
- [ ] Update `FunctionScheduleCRUD` to use new API (CDF-26692)

---

## Hosted Extractors Resources

### HostedExtractorSourceCRUD
- [ ] Create `HostedExtractorSourceRequest` and `HostedExtractorSourceResponse` pydantic models (CDF-26693)
- [ ] Create `HostedExtractorSourcesAPI` class and register in `ToolAPI` (CDF-26694)
- [ ] Update `HostedExtractorSourceCRUD` to use new API (CDF-26695)

### HostedExtractorDestinationCRUD
- [ ] Create `HostedExtractorDestinationRequest` and `HostedExtractorDestinationResponse` pydantic models (CDF-26696)
- [ ] Create `HostedExtractorDestinationsAPI` class and register in `ToolAPI` (CDF-26697)
- [ ] Update `HostedExtractorDestinationCRUD` to use new API (CDF-26698)

### HostedExtractorJobCRUD
- [ ] Create `HostedExtractorJobRequest` and `HostedExtractorJobResponse` pydantic models (CDF-26699)
- [ ] Create `HostedExtractorJobsAPI` class and register in `ToolAPI` (CDF-26700)
- [ ] Update `HostedExtractorJobCRUD` to use new API (CDF-26701)

### HostedExtractorMappingCRUD
- [ ] Create `HostedExtractorMappingRequest` and `HostedExtractorMappingResponse` pydantic models (CDF-26702)
- [ ] Create `HostedExtractorMappingsAPI` class and register in `ToolAPI` (CDF-26703)
- [ ] Update `HostedExtractorMappingCRUD` to use new API (CDF-26704)

---

## Industrial Tool Resources

### StreamlitCRUD
- [ ] Create `StreamlitRequest` and `StreamlitResponse` pydantic models (CDF-26705)
- [ ] Create `StreamlitAPI` class and register in `ToolAPI` (CDF-26706)
- [ ] Update `StreamlitCRUD` to use new API (CDF-26707)

---

## Location Resources

### LocationFilterCRUD
- [ ] Create `LocationFilterRequest` and `LocationFilterResponse` pydantic models (CDF-26708)
- [ ] Create `LocationFiltersAPI` class and register in `ToolAPI` (CDF-26709)
- [ ] Update `LocationFilterCRUD` to use new API (CDF-26710)

---

## Migration Resources

### ResourceViewMappingCRUD
- [ ] Create `ResourceViewMappingRequest` and `ResourceViewMappingResponse` pydantic models (CDF-26711)
- [ ] Create `ResourceViewMappingsAPI` class and register in `ToolAPI` (CDF-26712)
- [ ] Update `ResourceViewMappingCRUD` to use new API (CDF-26713)

---

## Raw Resources

### RawDatabaseCRUD
- [ ] Create `RawDatabaseRequest` and `RawDatabaseResponse` pydantic models (CDF-26714)
- [ ] Create `RawDatabasesAPI` class and register in `ToolAPI` (CDF-26715)
- [ ] Update `RawDatabaseCRUD` to use new API (CDF-26716)

### RawTableCRUD
- [ ] Create `RawTableRequest` and `RawTableResponse` pydantic models (CDF-26717)
- [ ] Create `RawTablesAPI` class and register in `ToolAPI` (CDF-26718)
- [ ] Update `RawTableCRUD` to use new API (CDF-26719)

---

## Relationship Resources

### RelationshipCRUD
- [ ] Create `RelationshipRequest` and `RelationshipResponse` pydantic models (CDF-26720)
- [ ] Create `RelationshipsAPI` class and register in `ToolAPI` (CDF-26721)
- [ ] Update `RelationshipCRUD` to use new API (CDF-26722)

---

## Robotics Resources

### RoboticFrameCRUD
- [ ] Create `RoboticFrameRequest` and `RoboticFrameResponse` pydantic models (CDF-26723)
- [ ] Create `RoboticFramesAPI` class and register in `ToolAPI` (CDF-26724)
- [ ] Update `RoboticFrameCRUD` to use new API (CDF-26725)

### RoboticLocationCRUD
- [ ] Create `RoboticLocationRequest` and `RoboticLocationResponse` pydantic models (CDF-26726)
- [ ] Create `RoboticLocationsAPI` class and register in `ToolAPI` (CDF-26727)
- [ ] Update `RoboticLocationCRUD` to use new API (CDF-26728)

### RoboticsDataPostProcessingCRUD
- [ ] Create `DataPostProcessingRequest` and `DataPostProcessingResponse` pydantic models (CDF-26729)
- [ ] Create `DataPostProcessingAPI` class and register in `ToolAPI` (CDF-26730)
- [ ] Update `RoboticsDataPostProcessingCRUD` to use new API (CDF-26731)

### RobotCapabilityCRUD
- [ ] Create `RobotCapabilityRequest` and `RobotCapabilityResponse` pydantic models (CDF-26732)
- [ ] Create `RobotCapabilitiesAPI` class and register in `ToolAPI` (CDF-26733)
- [ ] Update `RobotCapabilityCRUD` to use new API (CDF-26734)

### RoboticMapCRUD
- [ ] Create `RoboticMapRequest` and `RoboticMapResponse` pydantic models (CDF-26735)
- [ ] Create `RoboticMapsAPI` class and register in `ToolAPI` (CDF-26736)
- [ ] Update `RoboticMapCRUD` to use new API (CDF-26737)

---

## Streams Resources

### StreamCRUD
- [ ] Create `StreamRequest` and `StreamResponse` pydantic models (CDF-26738)
- [ ] Create `StreamsAPI` class and register in `ToolAPI` (CDF-26739)
- [ ] Update `StreamCRUD` to use new API (CDF-26740)

---

## Three D Model Resources

### ThreeDModelCRUD
- [ ] Create `ThreeDModelRequest` and `ThreeDModelResponse` pydantic models (CDF-26741)
- [ ] Create `ThreeDModelsAPI` class and register in `ToolAPI` (CDF-26742)
- [ ] Update `ThreeDModelCRUD` to use new API (CDF-26743)

---

## Timeseries Resources

### TimeSeriesCRUD
- [x] Create `TimeSeriesRequest` and `TimeSeriesResponse` pydantic models
- [ ] Create `TimeSeriesAPI` class and register in `ToolAPI` (CDF-26744)
- [ ] Update `TimeSeriesCRUD` to use new API (CDF-26745)

---

## Transformation Resources

### TransformationCRUD

- [ ] Create `TransformationRequest` and `TransformationResponse` pydantic models (CDF-26746)
- [ ] Create `TransformationsAPI` class and register in `ToolAPI` (CDF-26747)
- [ ] Update `TransformationCRUD` to use new API (CDF-26748)

---

## Workflow Resources

### WorkflowCRUD

- [ ] Create `WorkflowRequest` and `WorkflowResponse` pydantic models (CDF-26749)
- [ ] Create `WorkflowsAPI` class and register in `ToolAPI` (CDF-26750)
- [ ] Update `WorkflowCRUD` to use new API (CDF-26751)

### WorkflowVersionCRUD

- [ ] Create `WorkflowVersionRequest` and `WorkflowVersionResponse` pydantic models (CDF-26752)
- [ ] Create `WorkflowVersionsAPI` class and register in `ToolAPI` (CDF-26753)
- [ ] Update `WorkflowVersionCRUD` to use new API (CDF-26754)

### WorkflowTriggerCRUD

- [ ] Create `WorkflowTriggerRequest` and `WorkflowTriggerResponse` pydantic models (CDF-26755)
- [ ] Create `WorkflowTriggersAPI` class and register in `ToolAPI` (CDF-26756)
- [ ] Update `WorkflowTriggerCRUD` to use new API (CDF-26757)

---

## Agent Resources

### AgentCRUD

- [ ] Create `AgentRequest` and `AgentResponse` pydantic models (CDF-26758)
- [ ] Create `AgentsAPI` class and register in `ToolAPI` (CDF-26759)
- [ ] Update `AgentCRUD` to use new API (CDF-26760)
