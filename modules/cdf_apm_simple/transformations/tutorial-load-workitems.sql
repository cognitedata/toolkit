/* MAPPING_MODE_ENABLED: false */ /* {"version":1,"sourceType":"raw","mappings":[ {"from":"externalId","to":"externalId","asType":"STRING"}, {"from":"description","to":"description","asType":"STRING"}, {"from":"workOrder","to":"workOrder","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"}, {"from":"toBeDone","to":"toBeDone","asType":"BOOLEAN"}, {"from":"itemInfo","to":"itemInfo","asType":"STRING"}, {"from":"itemName","to":"itemName","asType":"STRING"}, {"from":"title","to":"title","asType":"STRING"}, {"from":"criticality","to":"criticality","asType":"STRING"}, {"from":"method","to":"method","asType":"STRING"}, {"from":"isCompleted","to":"isCompleted","asType":"BOOLEAN"}], "sourceLevel1":"tutorial_apm","sourceLevel2":"workitems"} */ select
  cast(`externalId` as STRING) as externalId,
  cast(`description` as STRING) as description,
  node_reference('tutorial_apm_simple', `workOrder`) as workOrder,
  cast(`toBeDone` as BOOLEAN) as toBeDone,
  cast(`itemInfo` as STRING) as itemInfo,
  cast(`itemName` as STRING) as itemName,
  cast(`title` as STRING) as title,
  cast(`criticality` as STRING) as criticality,
  cast(`method` as STRING) as method,
  cast(`isCompleted` as BOOLEAN) as isCompleted
from
  `tutorial_apm`.`workitems`;
