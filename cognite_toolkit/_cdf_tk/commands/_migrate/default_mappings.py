from functools import lru_cache

from cognite.client.data_classes.data_modeling import ViewId

from cognite_toolkit._cdf_tk.client.data_classes.migration import ResourceViewMappingApply

ASSET_ID = "cdf_asset_mapping"
EVENT_ID = "cdf_event_mapping"
TIME_SERIES_ID = "cdf_time_series_mapping"
TIME_SERIES_EXTRACTOR_ID = "cdf_time_series_extractor_mapping"
FILE_METADATA_ID = "cdf_file_metadata_mapping"
FILE_METADATA_EXTRACTOR_ID = "cdf_file_metadata_extractor_mapping"
FILE_ANNOTATIONS_ID = "cdf_file_annotations_mapping"
ASSET_ANNOTATIONS_ID = "cdf_asset_annotations_mapping"


@lru_cache(maxsize=1)
def create_default_mappings() -> list[ResourceViewMappingApply]:
    """Return the default mappings for migration."""
    ts_property_mapping = {
        "name": "name",
        "description": "description",
        "isStep": "isStep",
        "isString": "type",
        "unit": "sourceUnit",
        "unitExternalId": "unit",
    }
    file_property_mapping = {
        "name": "name",
        "source": "source",
        "labels": "tags",
        "mimeType": "mimeType",
        "directory": "directory",
        "sourceCreatedTime": "sourceCreatedTime",
        "sourceModifiedTime": "sourceUpdatedTime",
    }

    return [
        ResourceViewMappingApply(
            external_id=ASSET_ID,
            resource_type="asset",
            view_id=ViewId("cdf_cdm", "CogniteAsset", "v1"),
            property_mapping={
                "name": "name",
                "description": "description",
                "source": "source",
                "labels": "tags",
                "parentId": "parent",
            },
        ),
        ResourceViewMappingApply(
            external_id=EVENT_ID,
            resource_type="event",
            view_id=ViewId("cdf_cdm", "CogniteActivity", "v1"),
            property_mapping={
                "startTime": "startTime",
                "endTime": "endTime",
                "description": "description",
                "source": "source",
                "labels": "tags",
                "assetIds": "assets",
            },
        ),
        ResourceViewMappingApply(
            external_id=TIME_SERIES_ID,
            resource_type="timeseries",
            view_id=ViewId("cdf_cdm", "CogniteTimeSeries", "v1"),
            property_mapping=ts_property_mapping,
        ),
        ResourceViewMappingApply(
            external_id=TIME_SERIES_EXTRACTOR_ID,
            resource_type="timeseries",
            view_id=ViewId("cdf_extraction_extensions", "CogniteExtractorTimeSeries", "v1"),
            property_mapping={**ts_property_mapping, "metadata": "extractedData"},
        ),
        ResourceViewMappingApply(
            external_id=FILE_METADATA_ID,
            resource_type="file",
            view_id=ViewId("cdf_cdm", "CogniteFile", "v1"),
            property_mapping=file_property_mapping,
        ),
        ResourceViewMappingApply(
            external_id=FILE_METADATA_EXTRACTOR_ID,
            resource_type="file",
            view_id=ViewId("cdf_extraction_extensions", "CogniteExtractorFile", "v1"),
            property_mapping={**file_property_mapping, "metadata": "extractedData"},
        ),
        ResourceViewMappingApply(
            external_id=ASSET_ANNOTATIONS_ID,
            resource_type="assetAnnotation",
            view_id=ViewId("cdf_cdm", "CogniteDiagramAnnotation", "v1"),
            property_mapping={
                # We are ignoring the symbol region in the default mapping.
                "annotatedResource.id": "edge.startNode",
                "annotationType": "edge.type.externalId",
                "creatingUser": "sourceCreatedUser",
                "creatingApp": "sourceId",
                "creatingAppVersion": "sourceContext",
                "status": "status",
                "data.assetRef.id": "edge.endNode",
                "data.assetRef.externalId": "edge.endNode",
                "data.description": "description",
                "data.pageNumber": "startNodePageNumber",
                "data.textRegion.confidence": "confidence",
                "data.textRegion.xMin": "startNodeXMin",
                "data.textRegion.xMax": "startNodeXMax",
                "data.textRegion.yMin": "startNodeYMin",
                "data.textRegion.yMax": "startNodeYMax",
                "data.text": "startNodeText",
            },
        ),
        ResourceViewMappingApply(
            external_id=FILE_ANNOTATIONS_ID,
            resource_type="fileAnnotation",
            view_id=ViewId("cdf_cdm", "CogniteFileAnnotation", "v1"),
            property_mapping={
                "annotatedResource.id": "edge.startNode",
                "annotationType": "edge.type.externalId",
                "creatingUser": "sourceCreatedUser",
                "creatingApp": "sourceId",
                "creatingAppVersion": "sourceContext",
                "status": "status",
                "data.fileRef.id": "edge.startNode",
                "data.fileRef.externalId": "edge.startNode",
                "data.description": "description",
                "data.pageNumber": "startNodePageNumber",
                "data.textRegion.confidence": "confidence",
                "data.textRegion.xMin": "startNodeXMin",
                "data.textRegion.xMax": "startNodeXMax",
                "data.textRegion.yMin": "startNodeYMin",
                "data.textRegion.yMax": "startNodeYMax",
                "data.text": "startNodeText",
            },
        ),
    ]
