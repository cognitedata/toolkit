from functools import lru_cache

from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import ViewReference
from cognite_toolkit._cdf_tk.client.resource_classes.resource_view_mapping import ResourceViewMappingRequest

ASSET_ID = "cdf_asset_mapping"
EVENT_ID = "cdf_event_mapping"
TIME_SERIES_ID = "cdf_time_series_mapping"
TIME_SERIES_EXTRACTOR_ID = "cdf_time_series_extractor_mapping"
FILE_METADATA_ID = "cdf_file_metadata_mapping"
FILE_METADATA_EXTRACTOR_ID = "cdf_file_metadata_extractor_mapping"
FILE_ANNOTATIONS_ID = "cdf_file_annotations_mapping"
ASSET_ANNOTATIONS_ID = "cdf_asset_annotations_mapping"


@lru_cache(maxsize=1)
def create_default_mappings() -> list[ResourceViewMappingRequest]:
    """Return the default mappings for migration."""
    ts_property_mapping = {
        "name": "name",
        "description": "description",
        "isStep": "isStep",
        "isString": "type",
        "unit": "sourceUnit",
        "unitExternalId": "unit",
        "assetId": "assets",
    }
    file_property_mapping = {
        "name": "name",
        "source": "source",
        "labels": "tags",
        "mimeType": "mimeType",
        "directory": "directory",
        "assetIds": "assets",
        "sourceCreatedTime": "sourceCreatedTime",
        "sourceModifiedTime": "sourceUpdatedTime",
    }

    return [
        ResourceViewMappingRequest(
            external_id=ASSET_ID,
            resource_type="asset",
            view_id=ViewReference(space="cdf_cdm", external_id="CogniteAsset", version="v1"),
            property_mapping={
                "name": "name",
                "description": "description",
                "source": "source",
                "labels": "tags",
                "parentId": "parent",
            },
        ),
        ResourceViewMappingRequest(
            external_id=EVENT_ID,
            resource_type="event",
            view_id=ViewReference(space="cdf_cdm", external_id="CogniteActivity", version="v1"),
            property_mapping={
                "startTime": "startTime",
                "endTime": "endTime",
                "description": "description",
                "source": "source",
                "labels": "tags",
                "assetIds": "assets",
            },
        ),
        ResourceViewMappingRequest(
            external_id=TIME_SERIES_ID,
            resource_type="timeseries",
            view_id=ViewReference(space="cdf_cdm", external_id="CogniteTimeSeries", version="v1"),
            property_mapping=ts_property_mapping,
        ),
        ResourceViewMappingRequest(
            external_id=TIME_SERIES_EXTRACTOR_ID,
            resource_type="timeseries",
            view_id=ViewReference(
                space="cdf_extraction_extensions", external_id="CogniteExtractorTimeSeries", version="v1"
            ),
            property_mapping={**ts_property_mapping, "metadata": "extractedData"},
        ),
        ResourceViewMappingRequest(
            external_id=FILE_METADATA_ID,
            resource_type="file",
            view_id=ViewReference(space="cdf_cdm", external_id="CogniteFile", version="v1"),
            property_mapping=file_property_mapping,
        ),
        ResourceViewMappingRequest(
            external_id=FILE_METADATA_EXTRACTOR_ID,
            resource_type="file",
            view_id=ViewReference(space="cdf_extraction_extensions", external_id="CogniteExtractorFile", version="v1"),
            property_mapping={**file_property_mapping, "metadata": "extractedData"},
        ),
        ResourceViewMappingRequest(
            external_id=ASSET_ANNOTATIONS_ID,
            resource_type="assetAnnotation",
            view_id=ViewReference(space="cdf_cdm", external_id="CogniteDiagramAnnotation", version="v1"),
            property_mapping={
                # We are ignoring the symbol region in the default mapping.
                "annotatedResourceId": "edge.startNode",
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
        ResourceViewMappingRequest(
            external_id=FILE_ANNOTATIONS_ID,
            resource_type="fileAnnotation",
            view_id=ViewReference(space="cdf_cdm", external_id="CogniteDiagramAnnotation", version="v1"),
            property_mapping={
                "annotatedResourceId": "edge.startNode",
                "annotationType": "edge.type.externalId",
                "creatingUser": "sourceCreatedUser",
                "creatingApp": "sourceId",
                "creatingAppVersion": "sourceContext",
                "status": "status",
                "data.fileRef.id": "edge.endNode",
                "data.fileRef.externalId": "edge.endNode",
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
