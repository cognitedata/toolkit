select
  select
  cast(`externalId` as STRING) as externalId,
  cast(`externalId` as STRING) as name,
  cast(`description` as STRING) as description,
  cast(`parentExternalId` as STRING) as parentExternalId
from
  `{{raw_db}}`.`assets`;
