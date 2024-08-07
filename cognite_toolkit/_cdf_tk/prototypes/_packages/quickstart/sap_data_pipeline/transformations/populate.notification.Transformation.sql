SELECT
  CAST(`Notification` AS STRING) AS name,
  CAST(`Description` AS STRING) AS description,
  'workorder_mdi2_sap' AS source,
  CAST(`Notif.date` AS TIMESTAMP) AS sourceCreatedTime,
  CAST(`Planner group` AS STRING) AS sourceCreatedUser,
  CAST(`Notification` AS STRING) AS sourceId,
  CAST(`Notif.date` AS TIMESTAMP) AS sourceUpdatedTime,
  CAST(`User status` AS STRING) AS sourceUpdatedUser,
  CAST(`Required End` AS TIMESTAMP) AS endTime,
  CAST(`Required End` AS TIMESTAMP) AS scheduledEndTime,
  CAST(`Notif.date` AS TIMESTAMP) AS scheduledStartTime,
  CAST(`Notif.date` AS TIMESTAMP) AS startTime,
  CAST(`Notifictn type` AS STRING) AS priority,
  CAST(`Description.1` AS STRING) AS priorityDescription,
  CAST(`User status` AS STRING) AS status,
  CAST(`Notifictn type` AS STRING) AS type,
  CAST(`Notification` AS STRING) AS externalId
FROM
  `workorder_mdi2_sap`.`20231211 Project Cognite Orders en Nots_Notifications MDI-2 2023`;