select
  /* three first properties are required */
  cast(timeseries.`externalId` as STRING) as externalId, 
  cast(timeseries.`isStep` as BOOLEAN) as isStep,
  cast(timeseries.`type` as STRING) as type,
  /* direct relation */
  array(
    node_reference(
      '{{ instanceSpace }}',
      equipment_lookup.`externalId`
    )
  ) as equipment
from
  cdf_data_models(
    "cdf_idm",
    "CogniteProcessIndustries",
    "v1",
    "CogniteTimeSeries"
  ) as timeseries
left join cdf_data_models(
    "cdf_idm",
    "CogniteProcessIndustries",
    "v1",
    "CogniteEquipment"
  ) as equipment_lookup 
  /* update to the correct matching criteria for your data */
  on substring_index(replace(timeseries.`name`, 'VAL_', ''), ':', 1) == equipment_lookup.`name`
where
  timeseries.space == '{{ instanceSpace }}' and
  isnotnull(timeseries.`externalId`) and
  equipment_lookup.space == '{{ instanceSpace }}' and
  isnotnull(equipment_lookup.`externalId`)
