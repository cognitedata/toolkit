from pydantic import TypeAdapter

from cognite_toolkit._cdf_tk.client.resource_classes.view_to_view_mapping import ViewToViewMapping
from cognite_toolkit._cdf_tk.utils.file import read_yaml_content


def create_infield_data_mappings() -> dict[str, ViewToViewMapping]:
    mappings_data = """Action:
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
  propertyMapping:
    actionType: actionType
    conditionalActions: conditionalActions
    createdBy: createdBy
    isArchived: isArchived
    parameters: parameters
    target: target
    targetView: targetView
    updatedBy: updatedBy
    visibility: visibility
Asset:
  sourceView:
    space: cdf_core
    externalId: Asset
    version: v2
    type: view
  destinationView:
    space: infield_cdm_source_desc_sche_asset_file_ts
    externalId: Asset
    version: v1
    type: view
  propertyMapping:
    description: description
    labels: labels
    parent: parent
    path: path
    root: root
    source: source
    sourceCreatedTime: sourceCreatedTime
    sourceId: sourceId
    sourceUpdatedTime: sourceUpdatedTime
    title: title
CDF_User:
  sourceView:
    space: cdf_apps_shared
    externalId: CDF_User
    version: v1
    type: view
  destinationView:
    space: cdf_apps_shared
    externalId: CDF_User
    version: v1
    type: view
  propertyMapping:
    email: email
    name: name
    preferences: preferences
CDF_UserPreferences:
  sourceView:
    space: cdf_apps_shared
    externalId: CDF_UserPreferences
    version: v1
    type: view
  destinationView:
    space: cdf_apps_shared
    externalId: CDF_UserPreferences
    version: v1
    type: view
  propertyMapping:
    apmAppConfig: apmAppConfig
    language: language
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
  propertyMapping:
    assignedTo: assignedTo
    checklistItems: checklistItems
    createdBy: createdBy
    description: description
    endTime: endTime
    isArchived: isArchived
    labels: labels
    rootLocation: rootLocation
    solutionTags: solutionTags
    source: source
    sourceCreatedTime: sourceCreatedTime
    sourceId: sourceId
    sourceUpdatedTime: sourceUpdatedTime
    startTime: startTime
    status: status
    title: title
    type: type
    updatedBy: updatedBy
    visibility: visibility
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
  propertyMapping:
    asset: asset
    createdBy: createdBy
    description: description
    endTime: endTime
    files: files
    isArchived: isArchived
    labels: labels
    measurements: measurements
    note: note
    observations: observations
    order: order
    source: source
    sourceCreatedTime: sourceCreatedTime
    sourceId: sourceId
    sourceUpdatedTime: sourceUpdatedTime
    startTime: startTime
    status: status
    title: title
    updatedBy: updatedBy
    visibility: visibility
CogniteSolutionTag:
  sourceView:
    space: cdf_apps_shared
    externalId: CogniteSolutionTag
    version: v1
    type: view
  destinationView:
    space: cdf_apps_shared
    externalId: CogniteSolutionTag
    version: v1
    type: view
  propertyMapping:
    color: color
    description: description
    name: name
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
  propertyMapping:
    conditionalAction: conditionalAction
    createdBy: createdBy
    field: field
    isArchived: isArchived
    operator: operator
    source: source
    sourceView: sourceView
    updatedBy: updatedBy
    value: value
    visibility: visibility
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
  propertyMapping:
    createdBy: createdBy
    isArchived: isArchived
    logic: logic
    parentObject: parentObject
    updatedBy: updatedBy
    visibility: visibility
Creatable:
  sourceView:
    space: cdf_apps_shared
    externalId: Creatable
    version: v1
    type: view
  destinationView:
    space: cdf_apps_shared
    externalId: Creatable
    version: v1
    type: view
  propertyMapping:
    createdBy: createdBy
    isArchived: isArchived
    updatedBy: updatedBy
    visibility: visibility
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
  propertyMapping:
    createdBy: createdBy
    description: description
    isArchived: isArchived
    labels: labels
    max: max
    measuredAt: measuredAt
    min: min
    numericReading: numericReading
    options: options
    order: order
    stringReading: stringReading
    timeseries: timeseries
    title: title
    type: type
    updatedBy: updatedBy
    visibility: visibility
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
  propertyMapping:
    byDay: byDay
    byMonth: byMonth
    createdBy: createdBy
    endTime: endTime
    exceptionDates: exceptionDates
    freq: freq
    interval: interval
    isArchived: isArchived
    startTime: startTime
    status: status
    timezone: timezone
    until: until
    updatedBy: updatedBy
    visibility: visibility
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
  propertyMapping:
    assignedTo: assignedTo
    createdBy: createdBy
    description: description
    isArchived: isArchived
    labels: labels
    rootLocation: rootLocation
    solutionTags: solutionTags
    status: status
    templateItems: templateItems
    title: title
    updatedBy: updatedBy
    visibility: visibility
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
  propertyMapping:
    asset: asset
    createdBy: createdBy
    description: description
    isArchived: isArchived
    labels: labels
    measurements: measurements
    order: order
    schedules: schedules
    title: title
    updatedBy: updatedBy
    visibility: visibility
"""

    mappings_dict = read_yaml_content(mappings_data)
    return TypeAdapter(dict[str, ViewToViewMapping]).validate_python(mappings_dict)
