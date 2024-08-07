SELECT
  CAST(`FunctionalLocation` AS STRING) AS externalId,
  CAST(`FunctionalLocationName` AS STRING) AS description,
  CAST('sap.asset' AS STRING) AS sourceId,
  /*[x,y] as root*/ 
  CAST(`LastChangeDateTime` AS TIMESTAMP) AS sourceUpdatedTime,
  CAST(`AcquisitionDate` AS TIMESTAMP) AS sourceCreatedTime,
  if(`SuperiorFunctionalLocation` != "", node_reference('sp_idm_model', `SuperiorFunctionalLocation`), NULL)  AS parent
FROM
  `sap`.`asset`;