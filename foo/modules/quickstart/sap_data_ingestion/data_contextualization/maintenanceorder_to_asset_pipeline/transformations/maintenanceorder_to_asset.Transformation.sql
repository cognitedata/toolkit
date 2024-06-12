select CAST(`Order` AS STRING) AS externalId,
       node_reference(''{{ data_space }}'', CAST(`Equipment` AS STRING)) AS asset
from (
    (select
    *
    from `workorder_mdi2_sap`.`workorder_mdi2_sap_Work orders areas 100-200-600-700_Sheet1`)
    inner join (select
    `externalId` as target_external_id
    from cdf_nodes("'{{ model_space }}'", "MaintenanceOrder", "v1")
    ) as target_table on `Equipment` = target_table.target_external_id
    )
