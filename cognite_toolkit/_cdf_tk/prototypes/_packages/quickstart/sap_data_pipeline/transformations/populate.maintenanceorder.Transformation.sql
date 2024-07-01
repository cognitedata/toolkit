SELECT
  CAST(`Description` AS STRING) AS description,
  CAST(`Description.1` AS STRING) AS name,
  'workorder_mdi2_sap' AS source,
  CAST(`Created on` AS TIMESTAMP) AS sourceCreatedTime,
  CAST(`User status` AS STRING) AS sourceCreatedUser,
  CAST(`Functional Loc.` AS STRING) AS sourceId,
  CAST(`Basic fin. date` AS TIMESTAMP) AS sourceUpdatedTime,
  CAST(`User status` AS STRING) AS sourceUpdatedUser,
  CAST(`Basic fin. date` AS TIMESTAMP) AS endTime,
  CAST(`Basic fin. date` AS TIMESTAMP) AS scheduledEndTime,
  CAST(`Created on` AS TIMESTAMP) AS scheduledStartTime,
  CAST(`Created on` AS TIMESTAMP) AS startTime,
  CAST(`Priority` AS STRING) AS priority,
  CAST(`Priority` AS STRING) AS priorityDescription,
  CAST(`User status` AS STRING) AS status,
  CAST(`Plant section` AS STRING) AS type,
  CAST(`Order` AS STRING) AS externalId
FROM
  `workorder_mdi2_sap`.`workorder_mdi2_sap_Work orders areas 100-200-600-700_Sheet1`;