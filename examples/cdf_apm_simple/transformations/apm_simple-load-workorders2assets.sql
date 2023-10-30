select
  cast(`externalId` as STRING) as externalId,
  node_reference('{{datamodel}}', `sourceExternalId`) as startNode,
  node_reference('{{datamodel}}', `targetExternalId`) as endNode
from
  `{{raw_db}}`.`workorder2assets`;
