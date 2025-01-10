select
  /* three first properties are required */
  cast(activity.`externalId` as STRING) as externalId,
  /* direct relation */
  array(
    node_reference(
      '{{ instanceSpace }}',
      asset_lookup.`externalId`
    )
  ) as asset
from
  cdf_data_models(
    "cdf_cdm",
    "CogniteCore",
    "v1",
    "CogniteActivity"
  ) as activity
left join cdf_data_models(
    "cdf_cdm",
    "CogniteCore",
    "v1",
    "CogniteAsset"
  ) as asset_lookup
  /* update to the correct matching criteria for your data */
on
  activity.`tags`[0]  == asset_lookup.`name`
where
  activity.space == '{{ instanceSpace }}' and
  isnotnull(activity.tags) and
  asset_lookup.space == '{{ instanceSpace }}' and
  isnotnull(asset_lookup.`externalId`)
