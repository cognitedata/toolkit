select
  cast(`externalId` as STRING) as externalId,
  node_reference('cdfTemplate', `parentExternalId`) as parent,
  cast(`name` as STRING) as name,
  cast(`source` as STRING) as source,
  cast(`description` as STRING) as description,
  cast(`labels` as ARRAY < STRING >) as labels,
  to_json(`metadata`) as metadata
from
  `_cdf`.`assets`;
