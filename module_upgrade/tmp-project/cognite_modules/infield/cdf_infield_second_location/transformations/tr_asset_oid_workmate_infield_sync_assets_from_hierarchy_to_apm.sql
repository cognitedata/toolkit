--- Create All Assets without properties
--- This is necessary so we can populate the direct relations
--- in the next select statement
select
  cast(asset.externalId as STRING) as externalId,
  --- We skip the remaining properties,
  --- but need to have these columns set so we can do a UNION ALL operation with the statement below.
  null as parent,
  null as source,
  null as root,
  null as description,
  null as title,
  null as sourceId
from
  cdf_assetSubtree('{{second_root_asset_external_id}}') as asset

UNION ALL

--- Create All Assets with properties including direct relations
select
  cast(asset.externalId as STRING) as externalId,
  (case
    when isnull(asset.parentExternalId) then null
    else node_reference('sp_asset_{{second_location}}_source', asset.parentExternalId)
  end) as parent,
  cast("Asset Hierarachy" as STRING) as source,
  node_reference('sp_asset_{{second_location}}_source', cast(rootAsset.externalId as STRING)) as root,
  cast(asset.description as STRING) as description,
  cast(asset.name as STRING) as title,
  cast(asset.externalId as STRING) as sourceId
from
  cdf_assetSubtree('{{second_root_asset_external_id}}') as asset
  -- Get root asset
  inner join cdf_assetSubtree('{{second_root_asset_external_id}}') as rootAsset on asset.rootId = rootAsset.id
