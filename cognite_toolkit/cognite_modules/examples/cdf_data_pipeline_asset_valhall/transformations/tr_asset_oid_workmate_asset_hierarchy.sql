--
-- Create Asset Hierarchy using Transformation
--
-- Input data from RAW DB table (using example data)
--
-- Root node has parentExternal id = ''
-- Transformation is connected to asset data set
-- All metadata expect selected fileds are added to metadata
--
SELECT 
  sourceDb || ':' || tag          as externalId,
  if(parentTag is null, 
     '', 
     sourceDb || ':' ||parentTag) as parentExternalId,
  tag                             as name,
  sourceDb                        as source,
  description,
  dataset_id('{{asset_dataset}}')     as dataSetId,
  to_metadata_except(
    array("sourceDb", "parentTag", "description"), *) 
                                  as metadata
FROM 
  `{{asset_raw_input_db}}`.`{{asset_raw_input_table}}`
