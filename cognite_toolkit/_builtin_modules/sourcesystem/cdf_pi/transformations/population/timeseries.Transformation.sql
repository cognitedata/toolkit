select 
  externalId as externalId,
  name as name,
  'numeric' as type,
  false as isStep,
  if(try_get_unit(`unit`) IS NOT NULL, node_reference('cdf_cdm_units', try_get_unit(`unit`)), NULL) as unit,
  `unit` as sourceUnit
  
from `{{ rawSourceDatabase }}`.`timeseries_metadata`