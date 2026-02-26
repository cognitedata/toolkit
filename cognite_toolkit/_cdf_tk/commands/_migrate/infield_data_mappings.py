from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content


def create_infield_data_mappings() -> dict[str, ViewToViewMapping]:
    mappings_data = """
Action:
  sourceView:
    space: cdf_apm
    externalId: Action
    version: v1
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: Action
    version: v1
    type: view
  mapEqualNamedProperties: true
  propertyMapping:
    node.createdTime: sourceCreatedTime
    node.lastUpdatedTime: sourceUpdatedTime
Checklist:
  sourceView:
    space: cdf_apm
    externalId: Checklist
    version: v7
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: Checklist
    version: v1
    type: view
  mapEqualNamedProperties: true
  propertyMapping:
    node.createdTime: sourceCreatedTime
    node.lastUpdatedTime: sourceUpdatedTime
ChecklistItem:
  sourceView:
    space: cdf_apm
    externalId: ChecklistItem
    version: v7
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: ChecklistItem
    version: v1
    type: view
  mapEqualNamedProperties: true
  propertyMapping:
    node.createdTime: sourceCreatedTime
    node.lastUpdatedTime: sourceUpdatedTime
Condition:
  sourceView:
    space: cdf_apm
    externalId: Condition
    version: v1
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: Condition
    version: v1
    type: view
  mapEqualNamedProperties: true
  propertyMapping:
    node.createdTime: sourceCreatedTime
    node.lastUpdatedTime: sourceUpdatedTime
ConditionalAction:
  sourceView:
    space: cdf_apm
    externalId: ConditionalAction
    version: v1
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: ConditionalAction
    version: v1
    type: view
  mapEqualNamedProperties: true
  propertyMapping:
    node.createdTime: sourceCreatedTime
    node.lastUpdatedTime: sourceUpdatedTime
MeasurementReading:
  sourceView:
    space: cdf_apm
    externalId: MeasurementReading
    version: v4
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: MeasurementReading
    version: v1
    type: view
  mapEqualNamedProperties: true
  propertyMapping:
    node.createdTime: sourceCreatedTime
    node.lastUpdatedTime: sourceUpdatedTime
Schedule:
  sourceView:
    space: cdf_apm
    externalId: Schedule
    version: v4
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: Schedule
    version: v1
    type: view
  mapEqualNamedProperties: true
  propertyMapping:
    node.createdTime: sourceCreatedTime
    node.lastUpdatedTime: sourceUpdatedTime
Template:
  sourceView:
    space: cdf_apm
    externalId: Template
    version: v8
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: Template
    version: v1
    type: view
  mapEqualNamedProperties: true
  propertyMapping:
    node.createdTime: sourceCreatedTime
    node.lastUpdatedTime: sourceUpdatedTime
TemplateItem:
  sourceView:
    space: cdf_apm
    externalId: TemplateItem
    version: v7
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: TemplateItem
    version: v1
    type: view
  mapEqualNamedProperties: true
  propertyMapping:
    node.createdTime: sourceCreatedTime
    node.lastUpdatedTime: sourceUpdatedTime
"""

    mappings_dict = read_yaml_content(mappings_data.removeprefix("\n"))
    return TypeAdapter(dict[str, ViewToViewMapping]).validate_python(mappings_dict)
