select
  /* three first properties are required */
  cast(activity.`externalId` as STRING) as externalId,
  /* direct relation */
  collect_list(
    node_reference(
      '{{ instanceSpace }}',
      timeseries_lookup.`externalId`
    )
  ) as timeSeries
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
    "CogniteTimeSeries"
  ) as timeseries_lookup
  /* update to the correct matching criteria for your data */
on
  activity.`tags`[0]  = substring_index(replace(timeseries_lookup.`name`, 'VAL_', ''), ':', 1)
where
  activity.space == '{{ instanceSpace }}' and
  isnotnull(activity.tags) and
  isnotnull(activity.`externalId`) and
  timeseries_lookup.space == '{{ instanceSpace }}' and
  isnotnull(timeseries_lookup.`name`)
group by
  activity.`externalId`