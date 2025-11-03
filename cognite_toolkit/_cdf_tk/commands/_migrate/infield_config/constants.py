"""Constants for InField V2 config migration."""

from cognite.client.data_classes.data_modeling import ViewId

# View IDs for the new format
LOCATION_CONFIG_VIEW_ID = ViewId("infield_cdm_source_desc_sche_asset_file_ts", "InFieldLocationConfig", "v1")

# Target space for InFieldLocationConfig nodes
TARGET_SPACE = "APM_Config"

