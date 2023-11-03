select
  cast(asset.externalId as STRING) as externalId,
  (case
    when isnull(asset.parentExternalId) then null
    else node_reference('infield_default_location_source_data_space', asset.parentExternalId) 
  end) as parent,  
  cast("CDF Classic" as STRING) as source,
  node_reference('infield_default_location_source_data_space', cast(rootAsset.externalId as STRING)) as root,
  cast(asset.description as STRING) as description,
  cast(asset.name as STRING) as title,
  cast(asset.externalId as STRING) as sourceId,
from
  cdf_assetSubtree('WMT:VAL') as asset
  inner join cdf_assetSubtree('WMT:VAL') as rootAsset on asset.rootId = rootAsset.id