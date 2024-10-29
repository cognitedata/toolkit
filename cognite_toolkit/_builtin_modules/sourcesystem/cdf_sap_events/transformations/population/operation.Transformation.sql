select
  cast(`sourceId` as STRING) as externalId,
  cast(`WORKORDER_TASKNAME` as STRING) as name,
  cast(`WORKORDER_STATUS` as STRING) as status,
  array(cast(`WORKORDER_ITEMNAME` as STRING)) as tag
from
  `{{ rawDatabase }}`.`workitem`
where
  isnotnull(`sourceId`)
