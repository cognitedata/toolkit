SELECT
  CAST('data-dumps' AS STRING) AS source,
  CAST(`Created On` AS TIMESTAMP) AS sourceCreatedTime,
  CAST(`Created By` AS STRING) AS sourceCreatedUser,
  CAST(`Functional Loc.` AS STRING) AS sourceId,
  CAST(`Changed On` AS TIMESTAMP) AS sourceUpdatedTime,
  CAST(`Changed by` AS STRING) AS sourceUpdatedUser,
  CAST(`Description` AS STRING) AS description,
  CAST(`Functional Loc.` AS STRING) AS name,
  CAST(NULL AS TIMESTAMP) AS lastPathMaterializationTime,
  CAST(`Functional Loc.` AS STRING) AS externalId
FROM
  `data-dumps`.`data-dumps_dump FLOC RZ14`;