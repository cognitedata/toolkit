with assetid (
select
  first(wa.sourceExternalId) as workitemid,
  collect_list(ai.id)        as assetIds
from
  `workorder_oid_workmate`.`workitem2assets` wa,
  _cdf.assets                                ai
where
  ai.externalId = wa.targetExternalId
GROUP BY sourceExternalId)
select
  wo.externalId,
  wo.itemInfo,
  dataset_id('ds_transformations_oid')  as dataSetId,
  cast(from_unixtime(double(wo.`startTime`)/1000)
                         as TIMESTAMP)  as startTime,
  cast(from_unixtime(double(wo.`endTime`)/1000)
                         as TIMESTAMP)  as endTime,
  ai.assetIds                           as assetIds,
  "workitem"                            as type,
  "OID - workmate"                      as source,
  to_metadata_except(
    array("key",
          "startTime",
          "endTime",
          "externalId",
          "itemInfo",
          "assetIds"), *)              as metadata
from 
  `workorder_oid_workmate`.`workitems` wo,
  assetid ai
where
  ai.workitemid = wo.externalId
  and wo.`endTime` > wo.`startTime`