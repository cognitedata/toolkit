select
  cast(`externalId` as STRING) as externalId,
  cast(`description` as STRING) as description,
  node_reference('{{datamodel}}', `workOrder`) as workOrder,
  cast(`toBeDone` as BOOLEAN) as toBeDone,
  cast(`itemInfo` as STRING) as itemInfo,
  cast(`itemName` as STRING) as itemName,
  cast(`title` as STRING) as title,
  cast(`criticality` as STRING) as criticality,
  cast(`method` as STRING) as method,
  cast(`isCompleted` as BOOLEAN) as isCompleted
from
  `workorder_{{default_location}}_{{source_workorder}}`.`workitems`;
