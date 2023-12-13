--- Parents
select
  cast(asset.externalId as STRING) as externalId,
  (case
    when isnull(asset.parentExternalId) then null
    else node_reference('sp_asset_{{default_location}}_source', asset.parentExternalId)
  end) as parent,
  null as source,
  null as root,
  null as description,
  null as title,
  null as sourceId
from
  cdf_assetSubtree('{{root_asset_external_id}}') as asset
  inner join cdf_assetSubtree('{{root_asset_external_id}}') as rootAsset on asset.rootId = rootAsset.id

UNION ALL
--- Children
select
  cast(asset.externalId as STRING) as externalId,
  null as parent,
  cast("Asset Hierarachy" as STRING) as source,
  node_reference('sp_asset_{{default_location}}_source', cast(rootAsset.externalId as STRING)) as root,
  cast(asset.description as STRING) as description,
  cast(asset.name as STRING) as title,
  cast(asset.externalId as STRING) as sourceId
from
  cdf_assetSubtree('{{root_asset_external_id}}') as asset
  inner join cdf_assetSubtree('{{root_asset_external_id}}') as rootAsset on asset.rootId = rootAsset.id
