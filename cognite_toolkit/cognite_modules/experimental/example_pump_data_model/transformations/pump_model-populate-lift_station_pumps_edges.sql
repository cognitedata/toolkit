select
  concat(cast(`parent`.externalId as STRING), ':', cast(`externalId` as STRING)) as externalId,
  `parent` as startNode,
  node_reference('{{instance_space}}', cast(`externalId` as STRING)) as endNode
from
  cdf_data_models("{{source_model_space}}", "{{source_model}}", "1", "Asset")
where
  startswith(title, 'Pump')
