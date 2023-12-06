select
  cast(`externalId` as STRING) as externalId,
  cast(`categoryId` as INT) as categoryId,
  cast(`isCriticalLine` as BOOLEAN) as isCriticalLine,
  cast(`sourceDb` as STRING) as sourceDb,
  cast(`updatedDate` as TIMESTAMP) as updatedDate,
  cast(`createdDate` as TIMESTAMP) as createdDate,
  node_reference('{{datamodel}}', `parentExternalId`) as parent,
  cast(`description` as STRING) as description,
  cast(`tag` as STRING) as tag,
  cast(`areaId` as INT) as areaId,
  cast(`isActive` as BOOLEAN) as isActive
from
  `asset_{{default_location}}_{{source_asset}}`.`assets`;
