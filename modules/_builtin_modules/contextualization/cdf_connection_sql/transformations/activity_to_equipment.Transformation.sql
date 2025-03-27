select
  /* three first properties are required */
  cast(activity.`externalId` as STRING) as externalId,
  /* direct relation */
  array(
    node_reference(
      '{{ instanceSpace }}',
      equipment_lookup.`externalId`
    )
  ) as equipment
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
    "CogniteEquipment"
  ) as equipment_lookup
  /* update to the correct matching criteria for your data */
on
  activity.`tags`[0]  == equipment_lookup.`name`
where
  activity.space == '{{ instanceSpace }}' and
  isnotnull(activity.tags) and
  equipment_lookup.space == '{{ instanceSpace }}' and
  isnotnull(equipment_lookup.`externalId`)
