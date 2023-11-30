select
  cast(`externalId` as STRING) as externalId,
  node_reference('{{datamodel}}', `sourceExternalId`) as startNode,
  node_reference('{{datamodel}}', `targetExternalId`) as endNode
from
  `asset_{{default_location}}_{{source_asset}}`.`asset2children`;
