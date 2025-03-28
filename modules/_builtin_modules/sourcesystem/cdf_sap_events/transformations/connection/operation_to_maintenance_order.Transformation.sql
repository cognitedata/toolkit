/* The WORKORDER_NUMBER is not unique in the workorder table,
so use the following query to select the first match. */
with unique_workitem as (
  select
    *,
    row_number() over (partition by `sourceId` order by `sourceId`) as rn
  from
    {{ rawSourceDatabase }}.`workitem`
),
worder_unique as (
  select
    *,
    row_number() over (partition by `WORKORDER_NUMBER` order by `sourceId`) as rn
  from
    {{ rawSourceDatabase }}.`workorder`
)
select
  cast(task.`sourceId` as STRING) as externalId,
  node_reference('{{ instanceSpace }}', cast(worder.`sourceId` as STRING)) as maintenanceOrder
from
  unique_workitem as task
join
  worder_unique as worder
on
  task.`WORKORDER_NUMBER` = worder.`WORKORDER_NUMBER`
where
  isnotnull(task.`sourceId`) AND
  isnotnull(task.`WORKORDER_NUMBER`) AND
  isnotnull(worder.`WORKORDER_NUMBER`) AND
  isnotnull(worder.`sourceId`) AND
  worder.rn = 1 AND
  task.rn = 1
