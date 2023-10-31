select
  cast(`externalId` as STRING) as name,
  cast(`description` as STRING) as description,
  cast(`sourceDb` as STRING) as source,
  cast(`externalId` as STRING) as externalId,
  cast(`parentExternalId` as STRING) as parentExternalId
from
  `{{raw_db}}`.`assets`;
