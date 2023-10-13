/* MAPPING_MODE_ENABLED: false */
/* {"version":1,"sourceType":"raw","mappings":[ {"from":"asset","to":"externalId","asType":"STRING"}, {"to":"categoryId","asType":"INT"}, {"from":"","to":"isCriticalLine","asType":"BOOLEAN"}, {"from":"","to":"sourceDb","asType":"STRING"}, {"from":"","to":"metrics","asType":"ARRAY<STRING>"}, {"from":"","to":"updatedDate","asType":"TIMESTAMP"}, {"from":"","to":"createdDate","asType":"TIMESTAMP"}, {"from":"","to":"parent","asType":"STRUCT<`space`:STRING, `externalId`:STRING>"}, {"from":"","to":"description","asType":"STRING"}, {"from":"","to":"tag","asType":"STRING"}, {"from":"","to":"areaId","asType":"INT"}, {"from":"","to":"isActive","asType":"BOOLEAN"}], "sourceLevel1":"tutorial_apm","sourceLevel2":"timeseries2assets"} */
select
  cast(`asset` as STRING) as externalId,
  array(timeseries) as metrics
from
  `tutorial_apm`.`timeseries2assets`;
