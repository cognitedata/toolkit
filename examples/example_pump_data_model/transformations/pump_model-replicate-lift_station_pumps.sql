select
  concat(cast(`parent` as STRING), ':', cast(`externalId` as STRING)) as externalId,
  `parent` as startNode,
  node_reference('{{instance_space}}', cast(`externalId` as STRING)) as endNode
from
  cdf_data_models("{{model_space}}", "AssetHierarchy", "1", "Asset")
where
  startswith(name, 'Pump')
