
select
  CAST(`Functional Loc.` AS STRING) AS externalId,
  node_reference('sp_sap_maia', CAST(`SupFunctLoc.` AS STRING)) AS parent
from `data-dumps`.`data-dumps_dump FLOC RZ14`
