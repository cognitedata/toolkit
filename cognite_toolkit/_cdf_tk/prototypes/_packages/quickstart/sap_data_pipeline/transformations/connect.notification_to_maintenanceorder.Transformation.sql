select CAST(`Notification` AS STRING) AS externalId,
       node_reference('sp_{{ source }}_{{ location }}_data', CAST(`Notification` AS STRING)) AS maintenanceOrder
from (
    (select
    *
    from `workorder_mdi2_sap`.`20231211 Project Cognite Orders en Nots_Notifications MDI-2 2023`)
    inner join (select
    `externalId` as target_external_id
    from cdf_nodes("{{ idm_model_space }}", "Notification", "v1")
    ) as target_table on `Notification` = target_table.target_external_id
    )
