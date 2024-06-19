-- Root Asset
-- The asset must be set up in hierarchical order as the container for the parent asset requires the
-- parent asset to be created first.

select
  cast(`externalId` as STRING) as externalId,
  null as parent,
  node_reference('{{instance_space}}', '{{root_asset_external_id}}') as root,
  cast(`name` as STRING) as title,
  cast(`source` as STRING) as source,
  cast(`description` as STRING) as description,
  cast(`labels` as ARRAY < STRING >) as labels,
  to_json(`metadata`) as metadata
from
  cdf_assetSubtree("{{root_asset_external_id}}")
where
-- The root asset is created with a null parentExternalId.
  isnull(`parentExternalId`)

UNION ALL
-- Pump Stations
select
  cast(`externalId` as STRING) as externalId,
  node_reference('{{instance_space}}', `parentExternalId`) as parent,
  node_reference('{{instance_space}}', '{{root_asset_external_id}}') as root,
  cast(`name` as STRING) as title,
  cast(`source` as STRING) as source,
  cast(`description` as STRING) as description,
  cast(`labels` as ARRAY < STRING >) as labels,
  to_json(`metadata`) as metadata
from
  cdf_assetSubtree('{{root_asset_external_id}}')
where
-- This is used to select the Lift Stations.
 isnotnull(`externalId`) and isnotnull(`parentExternalId`) and not startswith(name, 'Pump')

UNION ALL
-- Pumps
select
  concat('pump:', cast(`externalId` as STRING)) as externalId,
  node_reference('{{instance_space}}', `parentExternalId`) as parent,
  node_reference('{{instance_space}}', '{{root_asset_external_id}}') as root,
  cast(`name` as STRING) as title,
  cast(`source` as STRING) as source,
  cast(`description` as STRING) as description,
  cast(`labels` as ARRAY < STRING >) as labels,
  to_json(`metadata`) as metadata
from
  cdf_assetSubtree('{{root_asset_external_id}}')
where
-- This is used to select the Pumps.
 isnotnull(`externalId`) and isnotnull(`parentExternalId`) and startswith(name, 'Pump');
