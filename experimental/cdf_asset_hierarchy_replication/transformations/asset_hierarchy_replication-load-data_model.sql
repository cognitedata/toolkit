select
  cast(`externalId` as STRING) as externalId,
  node_reference('{{space}}', `parentExternalId`) as parent,
  node_reference('{{space}}', '{{root_asset_external_id}}') as root,
  cast(`name` as STRING) as name,
  cast(`source` as STRING) as source,
  cast(`description` as STRING) as description,
  cast(`labels` as ARRAY < STRING >) as labels,
  to_json(`metadata`) as metadata
from
  cdf_assetSubtree('{{root_asset_external_id}}')
where
 isnotnull(`externalId`) and isnotnull(`parentExternalId`);
