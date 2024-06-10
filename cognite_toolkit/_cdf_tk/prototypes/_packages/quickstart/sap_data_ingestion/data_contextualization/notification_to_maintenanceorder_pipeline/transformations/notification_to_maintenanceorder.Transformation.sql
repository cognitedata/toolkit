select CAST(`Notification` AS STRING) AS externalId,
       node_reference('sp_sap_maia', CAST(`Notification` AS STRING)) AS maintenanceOrder
from (
    (select
    *
    from `workorder_mdi2_sap`.`20231211 Project Cognite Orders en Nots_Notifications MDI-2 2023`)
    inner join (select
    `externalId` as target_external_id
    from cdf_nodes("sp_idm_model", "Notification", "v1")
    ) as target_table on `Notification` = target_table.target_external_id
    )
