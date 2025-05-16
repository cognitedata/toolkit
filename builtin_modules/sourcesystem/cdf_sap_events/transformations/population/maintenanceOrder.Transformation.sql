/* MAPPING_MODE_ENABLED: false */
/* {"version":1,"sourceType":"raw","mappings":[{"from":"externalId","to":"externalId","asType":"STRING"},{"from":"","to":"aliases","asType":"ARRAY<STRING>"},{"from":"name","to":"name","asType":"STRING"},{"from":"","to":"source","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"","to":"path","asType":"ARRAY<STRUCT<`space`:STRING, `externalId`:STRING>>"},{"from":"","to":"root","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"","to":"assetClass","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"","to":"sourceUpdatedUser","asType":"STRING"},{"from":"","to":"pathLastUpdatedTime","asType":"TIMESTAMP"},{"from":"","to":"sourceUpdatedTime","asType":"TIMESTAMP"},{"from":"","to":"type","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"","to":"sourceCreatedUser","asType":"STRING"},{"from":"","to":"parent","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"description","to":"description","asType":"STRING"},{"from":"","to":"tags","asType":"ARRAY<STRING>"},{"from":"","to":"sourceCreatedTime","asType":"TIMESTAMP"},{"from":"","to":"sourceContext","asType":"STRING"},{"from":"","to":"sourceId","asType":"STRING"},{"from":"","to":"object3D","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"}],"sourceLevel1":"sap_staging","sourceLevel2":"dump"} */
select
  cast(`sourceId` as STRING) as externalId,
  cast(`WORKORDER_DESC` as STRING) as description,
  cast(`WORKORDER_TITLE` as STRING) as name,
  cast(`WORKORDER_STATUS` as STRING) as status,
  cast(`WORKORDER_SCHEDULEDSTART` as TIMESTAMP) as scheduledStartTime,
  cast(`WORKORDER_DUEDATE` as TIMESTAMP) as scheduledEndTime,
  cast(`WORKORDER_PLANNEDSTART` as TIMESTAMP) as startTime,
  cast(`WORKORDER_COMPLETIONDATE` as TIMESTAMP) as endTime,
  cast(`WORKORDER_CREATEDDATE` as TIMESTAMP) as sourceCreatedTime,
  cast(`WORKORDER_MAITENANCETYPE` as STRING) as type,
  cast(`WORKORDER_PRIORITYDESC` as STRING) as priorityDescription
from
  `{{ rawSourceDatabase }}`.`workorder`
where
  isnotnull(`sourceId`)
