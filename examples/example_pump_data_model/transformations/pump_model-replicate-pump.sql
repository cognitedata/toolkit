select
  cast(`externalId` as STRING) as externalId,
  cast(`name` as STRING) as name,
  cast(`description` as STRING) as description,
  cast(get_json_object(`metadata`, '$.DesignPointHeadFT') as DOUBLE) as DesignPointHeadFT,
  cast(get_json_object(`metadata`, '$.LowHeadFT') as DOUBLE) as LowHeadFT,
  cast(get_json_object(`metadata`, '$.DesignPointFlowGPM') as DOUBLE) as DesignPointFlowGPM,
  cast(get_json_object(`metadata`, '$.LowHeadFlowGPM') as DOUBLE) as LowHeadFlowGPM,
  parent as liftStation
from
  cdf_data_models("{{model_space}}", "AssetHierarchy", "1", "Asset")
where
  startswith(name, 'Pump')
