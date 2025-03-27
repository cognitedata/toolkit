select
  /* three first properties are required */
  cast(timeseries.`externalId` as STRING) as externalId,
  cast(timeseries.`isStep` as BOOLEAN) as isStep,
  cast(timeseries.`type` as STRING) as type,
  /* direct relation */
  array(
    node_reference(
      '{{ instanceSpace }}',
      asset_lookup.`externalId`
    )
  ) as assets
from
  cdf_data_models(
    "cdf_cdm",
    "CogniteCore",
    "v1",
    "CogniteTimeSeries"
  ) as timeseries
left join cdf_data_models(
    "cdf_cdm",
    "CogniteCore",
    "v1",
    "CogniteAsset"
  ) as asset_lookup
  /* update to the correct matching criteria for your data */
  on substring_index(replace(timeseries.`name`, 'VAL_', ''), ':', 1) == asset_lookup.`name`
where
  timeseries.space == '{{ instanceSpace }}' and
  isnotnull(timeseries.`externalId`) and
  asset_lookup.space == '{{ instanceSpace }}' and
  isnotnull(asset_lookup.`externalId`)
