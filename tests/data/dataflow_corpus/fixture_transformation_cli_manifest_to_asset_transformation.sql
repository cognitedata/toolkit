-- COPY ALL ASSET-NODES
SELECT
    name,
    176403360399497 AS dataSetId,
    externalId,
    parentExternalId,
    description,
    source,
    metadata
FROM
    _cdf.assets
WHERE
    parentExternalId == "so_route"
    AND dataSetId == dataset_id("uc_002")
    AND is_new("1.last_version_#230322", createdTime)
