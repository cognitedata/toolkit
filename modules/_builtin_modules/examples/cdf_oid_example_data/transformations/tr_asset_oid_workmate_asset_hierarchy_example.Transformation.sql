select
  cast(`externalId` as STRING) as externalId,
  cast(`tag` as STRING) as name,
  cast(`description` as STRING) as description,
  cast(`sourceDb` as STRING) as source,
  cast(`parentExternalId` as STRING) as parentExternalId
from
  `asset_{{default_location}}_{{source_asset}}`.`assets`;
