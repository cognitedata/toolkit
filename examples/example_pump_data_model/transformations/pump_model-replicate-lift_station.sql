select
  cast(`externalId` as STRING) as externalId,
  cast(`name` as STRING) as name,
  cast(`description` as STRING) as description,
  'liftStation' as assetType
from
  cdf_data_models("{{model_space}}", "AssetHierarchy", "1", "Asset")
where
  -- Bug in the transformation not allowing startswith(externalId, 'lift_station:')
  not startswith(name, 'Pump')
