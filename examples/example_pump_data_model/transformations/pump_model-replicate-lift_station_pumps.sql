select
  concat(cast(`parent` as STRING), ':', cast(`externalId` as STRING)) as externalId,
  `parent` as startNode,
  node_reference('{{space}}', cast(`externalId` as STRING)) as endNode
from
  cdf_data_models("{{space}}", "AssetHierarchy", "1", "Asset")
where
  startswith(name, 'Pump')
