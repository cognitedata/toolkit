from functools import lru_cache

from cognite.client.data_classes.data_modeling import ViewId

from cognite_toolkit._cdf_tk.client.data_classes.migration import ResourceViewMappingApply

_ASSET_ID = "cdf_asset_mapping"
_EVENT_ID = "cdf_event_mapping"
_TIME_SERIES_ID = "cdf_time_series_mapping"
_FILE_METADATA_ID = "cdf_file_metadata_mapping"


@lru_cache(maxsize=1)
def create_default_mappings() -> list[ResourceViewMappingApply]:
    """Return the default mappings for migration."""
    return [
        ResourceViewMappingApply(
            external_id=_ASSET_ID,
            resource_type="asset",
            view_id=ViewId("cdf_cdm", "CogniteAsset", "v1"),
            property_mapping={
                "name": "name",
                "description": "description",
                "source": "source",
                "labels": "tags",
            },
        ),
        ResourceViewMappingApply(
            external_id=_EVENT_ID,
            resource_type="event",
            view_id=ViewId("cdf_cdm", "CogniteActivity", "v1"),
            property_mapping={
                "startTime": "startTime",
                "endTime": "endTime",
                "description": "description",
                "source": "source",
                "labels": "tags",
            },
        ),
        ResourceViewMappingApply(
            external_id=_TIME_SERIES_ID,
            resource_type="timeseries",
            view_id=ViewId("cdf_cdm", "CogniteTimeSeries", "v1"),
            property_mapping={
                "name": "name",
                "description": "description",
                "isStep": "isStep",
                "isString": "type",
                "legacyName": "alias",
                "unit": "sourceUnit",
                "unitExternalId": "unit",
                "source": "source",
            },
        ),
        ResourceViewMappingApply(
            external_id=_FILE_METADATA_ID,
            resource_type="file",
            view_id=ViewId("cdf_cdm", "CogniteFile", "v1"),
            property_mapping={
                "name": "name",
                "description": "description",
                "source": "source",
                "labels": "tags",
                "mimeType": "mimeType",
                "directory": "directory",
                "sourceCreatedTime": "sourceCreatedTime",
                "sourceUpdatedTime": "sourceUpdatedTime",
            },
        ),
    ]
