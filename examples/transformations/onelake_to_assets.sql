SELECT
    cast(id AS STRING) AS externalId,
    name AS name
FROM ext_onelake('fabric-lakehouse-prod', 'assets')
