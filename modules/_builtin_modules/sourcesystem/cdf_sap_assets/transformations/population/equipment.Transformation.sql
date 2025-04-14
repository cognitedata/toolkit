/* MAPPING_MODE_ENABLED: false */
/* {"version":1,"sourceType":"raw","mappings":[{"from":"externalId","to":"externalId","asType":"STRING"},{"from":"","to":"aliases","asType":"ARRAY<STRING>"},{"from":"name","to":"name","asType":"STRING"},{"from":"","to":"source","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"","to":"path","asType":"ARRAY<STRUCT<`space`:STRING, `externalId`:STRING>>"},{"from":"","to":"root","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"","to":"assetClass","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"","to":"sourceUpdatedUser","asType":"STRING"},{"from":"","to":"pathLastUpdatedTime","asType":"TIMESTAMP"},{"from":"","to":"sourceUpdatedTime","asType":"TIMESTAMP"},{"from":"","to":"type","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"","to":"sourceCreatedUser","asType":"STRING"},{"from":"","to":"parent","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"},{"from":"description","to":"description","asType":"STRING"},{"from":"","to":"tags","asType":"ARRAY<STRING>"},{"from":"","to":"sourceCreatedTime","asType":"TIMESTAMP"},{"from":"","to":"sourceContext","asType":"STRING"},{"from":"","to":"sourceId","asType":"STRING"},{"from":"","to":"object3D","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"}],"sourceLevel1":"sap_staging","sourceLevel2":"dump"} */
select
  concat('WMT:', cast(`WMT_TAG_NAME` as STRING)) as externalId,
  cast(`WMT_TAG_NAME` as STRING) as name,
  cast(`WMT_TAG_DESC` as STRING) as description,
  cast(`WMT_TAG_ID` as STRING) as sourceId, 
  cast(`WMT_TAG_CREATED_DATE` as TIMESTAMP) as sourceCreatedTime,
  cast(`WMT_TAG_UPDATED_DATE` as TIMESTAMP) as sourceUpdatedTime,
  cast(`WMT_TAG_UPDATED_BY` as STRING) as sourceUpdatedUser,
  cast(`WMT_CONTRACTOR_ID` as STRING) as manufacturer,
  cast(`WMT_TAG_GLOBALID` as STRING) as serialNumber
from
  `{{ rawSourceDatabase }}`.`dump`
where
  isnotnull(`WMT_TAG_NAME`) AND
  cast(`WMT_CATEGORY_ID` as INT) != 1157
