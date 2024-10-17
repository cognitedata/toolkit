select
  cast(task.`sourceId` as STRING) as externalId,
  node_reference('{{ instanceSpace }}',  cast(worder.`sourceId` as STRING)) as maintenanceOrder
from
    {{ rawDatabase }}.`worktask` as task
join
  {{ rawDatabase }}.`workorder` as worder
on
  task.`WORKORDER_NUMBER` = worder.`WORKORDER_NUMBER`
where
  isnotnull(task.`sourceId`) AND
  isnotnull(task.`WORKORDER_NUMBER`) AND
  isnotnull(worder.`WORKORDER_NUMBER`) AND
  isnotnull(worder.`sourceId`)
