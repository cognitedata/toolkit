select
  cast(`sourceId` as STRING) as externalId,
  cast(`WORKORDER_TASKNAME` as STRING) as name,
  cast(`WORKORDER_TASKDESC` as STRING) as description,
  cast(`WORKORDER_TASKCOMPLETEDDATE` as TIMESTAMP) as endTime,
  cast(`WORKORDER_TASKDISCIPLINEDESC` as STRING) as mainDiscipline
from
  `{{ rawDatabase }}`.`worktask`
where
  isnotnull(`sourceId`)
