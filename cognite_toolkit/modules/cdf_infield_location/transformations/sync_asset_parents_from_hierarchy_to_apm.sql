select
  cast(asset.externalId as STRING) as externalId,
  (case
    when isnull(asset.parentExternalId) then null
    else node_reference('infield_{{location_name}}_location_source_data_space', asset.parentExternalId) 
  end) as parent
from
  cdf_assetSubtree('{{root_asset_external_id}}') as asset
  inner join cdf_assetSubtree('{{root_asset_external_id}}') as rootAsset on asset.rootId = rootAsset.id