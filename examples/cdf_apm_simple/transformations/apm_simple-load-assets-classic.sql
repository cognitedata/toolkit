select
  cast(`externalId` as STRING) as externalId,
  cast(`externalId` as STRING) as name,
  cast(`description` as STRING) as description,
from
  `{{raw_db}}`.`assets`;
