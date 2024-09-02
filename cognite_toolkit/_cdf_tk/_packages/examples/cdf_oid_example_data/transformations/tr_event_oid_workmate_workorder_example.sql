with assetid (
select
  first(wa.sourceExternalId) as workorderid,
  collect_list(ai.id)        as assetIds
from
  `workorder_oid_workmate`.`workorder2assets` wa,
  _cdf.assets                                 ai
where
  ai.externalId = wa.targetExternalId
GROUP BY sourceExternalId)
select
  wo.externalId,
  wo.description,
  dataset_id('ds_transformations_oid')  as dataSetId,
  cast(from_unixtime(double(wo.`startTime`)/1000)
                         as TIMESTAMP)  as startTime,
  cast(from_unixtime(double(wo.`endTime`)/1000)
                         as TIMESTAMP)  as endTime,
  ai.assetIds                           as assetIds,
  "workorder"                           as type,
  "OID - workmate"                      as source,
  to_metadata_except(
    array("key",
          "startTime",
          "endTime",
          "externalId",
          "wo.description",
          "assetIds"), *)              as metadata
from
  `workorder_oid_workmate`.`workorders` wo,
  assetid ai
where
  ai.workorderid = wo.workOrderNumber
  and wo.`endTime` > wo.`startTime`
