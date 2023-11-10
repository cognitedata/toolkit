select
  cast(asset.externalId as STRING) as externalId, 
  cast("Asset Hierarachy" as STRING) as source,
  node_reference('infield_{{location_name}}_location_source_data_space', cast(rootAsset.externalId as STRING)) as root,
  cast(asset.description as STRING) as description,
  cast(asset.name as STRING) as title,
  cast(asset.externalId as STRING) as sourceId
from
  cdf_assetSubtree('{{root_asset_external_id}}') as asset
  inner join cdf_assetSubtree('{{root_asset_external_id}}') as rootAsset on asset.rootId = rootAsset.id