"""Constants for InField V2 config migration."""

from cognite.client.data_classes.data_modeling import ViewId
from cognite.client.data_classes.data_modeling.ids import DataModelId

# View IDs for the new format
LOCATION_CONFIG_VIEW_ID = ViewId("infield_cdm_source_desc_sche_asset_file_ts", "InFieldLocationConfig", "v1")

# Target space for InFieldLocationConfig nodes
TARGET_SPACE = "APM_Config"

# Default data model for LocationFilter
DEFAULT_LOCATION_FILTER_DATA_MODEL = DataModelId(
    space="infield_cdm_source_desc_sche_asset_file_ts",
    external_id="InFieldOnCDM",
    version="v1",
)

