select
  cast(`externalId` as STRING) as externalId,
  cast(get_json_object(`metadata`, '$.DesignPointHeadFT') as DOUBLE) as DesignPointHeadFT,
  cast(get_json_object(`metadata`, '$.LowHeadFT') as DOUBLE) as LowHeadFT,
  cast(get_json_object(`metadata`, '$.DesignPointFlowGPM') as DOUBLE) as DesignPointFlowGPM,
  cast(get_json_object(`metadata`, '$.LowHeadFlowGPM') as DOUBLE) as LowHeadFlowGPM
from
  cdf_data_models("{{source_model_space}}", "{{source_model}}", "1", "Asset")
where
  startswith(title, 'Pump')
