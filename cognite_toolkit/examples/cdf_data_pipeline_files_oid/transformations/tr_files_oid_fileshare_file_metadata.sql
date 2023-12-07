--
-- Update file metdata using Transformation
--
-- Input data from RAW DB table (using example data) and uploaded files
--
SELECT
  file.id                         as id,
  file.name                       as name,
  'valhall:'|| file.name          as  externalId,
  meta.source                     as source,
  meta.`mime_type`                as mimeType,
  dataset_id("ds-files:valhall")  as dataSetId,
  to_metadata(doc_type)           as metadata
FROM 
  `src-files-valhall-fileshare`.`files_meta` meta,
  _cdf.files                                 file 
WHERE 
  file.name = meta.name
