-- The source data has duplicates this filters them out.
with unique_workitem as (
  select
    *,
    row_number() over (partition by `sourceId` order by `sourceId`) as rn
  from
    `{{ rawDatabase }}`.`workitem`
)
select
  cast(`sourceId` as STRING) as externalId,
  cast(`WORKORDER_TASKNAME` as STRING) as name,
  cast(`WORKORDER_STATUS` as STRING) as status,
  array(cast(`WORKORDER_ITEMNAME` as STRING)) as tag
from
  unique_workitem
where
  isnotnull(`sourceId`) and
  rn = 1