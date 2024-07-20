# cdf_data_pipeline_3d_valhall

This module relies on these example data modules being loaded:

- cdf_oid_example_data
- cdf_data_pipeline_asset_valhall
- The Valhall 3D model uploaded and processed by CDF 3D pipeline

The module creates a simple data pipeline for contextualization of a 3D model (Valhall) with
the asset data based on the OID example module. In addition, the module contains and example
File extractor configuration that can be used to upload the 3D model from your file system.

Processing workflow for contextualization are:
![image](https://github.com/cognitedata/toolkit/assets/31886431/b29522f8-7f4b-4e23-b06a-f3ffffde103c)

## Test data

The data for the mapping are from the module `cdf_oid_example_data` is a small data set
from [Open IndustrialData](https://learn.cognite.com/open-industrial-data), the Valhall platform

The 3D model is from the Valhall asset, and can be provided on request.  
For the test, the model was uploaded to CDF using the UI and the 3D pipeline in CDF.

## Managed resources

This module manages the following resources:

1. auth:
   - Name: `gp_3d_oid_extractor`
     - Content: Authorization group used by the extractor writing data to CDF files and RAW,
       extraction pipeline and update of the extraction pipeline run
   - Name: `gp_3d_oid_processing`
     - Content: Authorization group used for contextualization of 3d function, read/write
       to raw, files and pipeline runs
   - Name: `gp_3d_oid_read`
     - Content: Authorization group used by the users reading 3d model

2. data set:
   - ID: `ds_3d_oid`
     - Content: Data lineage, used Links to used extraction pipelines, transformation and raw tables

3. extraction pipeline:
   - ID: `ep_src_3d_oid_fileshare`
     - Content: Documentation and configuration example for the file extractor with connection
       to local disk for upload of 3d file
   - ID: `ep_ctx_3d_oid_fileshare_annotation`
     - Content: Documentation and configuration for a CDF function running example 3D and asset
       contextualization (see function form more description)

4. functions:
   - ID: `fn_context_3d_oid_fileshare_asset`

    The contextualization function will then in `normal`operation read configuration and start process by:

    - Read RAW table with manual mappings and extract all rows not contextualized
    - Apply manual mappings from 3D nodes to Asset - this will overwrite any existing mapping
    - Read all time series not matched (or all if runAll is True)
    - Read all assets
    - Run ML contextualization to match 3D Nodes -> Assets
    - Update 3D Nodes with mapping
    - Write results matched (good) not matched (bad) to RAW
    - Output in good/bad table can then be used in workflow to update manual mappings

5. raw:
   - db/table: `3d_oid_opcua/contextualization_good`
     - Content: result from contextualization with mappings that was added to CDF between 3d and assets.
       Table can be used for quality control and input to manual tuning
   - db/table: `3d_oid_opcua/contextualization_bad`
     - Content: result from contextualization with mappings NOT added to CDF between 3d and assets.
       Table can be used for quality control and input to manual tuning
   - db/table: `3d_oid_opcua/contextualization_manual_input`
     - Content: manual input to the contextualization process. Table can be used as:
       - a migration table reads existing mappings from the old project and insert to this table
         to re-map in the new project
       - manual tuning based on results from process - wrong mappings in good/bad table can be
         corrected by adding it to this table

### Illustration of the time 3d data pipeline

![image](https://github.com/cognitedata/toolkit/assets/31886431/f1129181-bab0-42cb-8366-860e8fb30d7e)

### Illustration of the contextualization workflow

With usage of the output stored in the good /bad table to process false positive or just manually map
content not able to be processed automatically, you can create a contextualization workflow.
Included in the workflow, there could also be a rule module using properties of the data or external
input to map content, as illustrated in the illustration below:

![image](https://github.com/cognitedata/toolkit/assets/31886431/0e990b47-0c06-4040-b680-7e2dddcdccee)

## Variables

The following variables are required and defined in this module:

| Variable                               | Description                                                                                                              |
|----------------------------------------|--------------------------------------------------------------------------------------------------------------------------|
| location_name                          | The location for your data, name used in all resource type related to data pipeline. We use oid (Open Industrial Data)   |
| source_name                            | The name of the source making it possible to identify where the data originates from, ex: 'workmate', 'sap', 'oracle',.. |
| 3d_dataset                             | The name of data set used for time series in this example, must correspond to name used in example data                  |
| 3d_model_name                          | The name of 3d name used in example data                                                                                 |
| external_root_id_asset                 | The external ID for the Asset name used in example data                                                                  |
| 3d_location_extractor_group_source_id  | Object/ Source ID for security group in IdP. Used to run integration/extractor                                           |
| 3d_location_processing_group_source_id | Object/ Source ID for security group in IdP. Used to run CDF processing/contextualization                                |
| 3d_location_read_group_source_id       | Object/ Source ID for security group in IdP. Used to read file data                                                      |

## Usage

You should copy and rename an example module into the `custom_modules` directory (remove any `cdf_` prefixes) and make
your own modifications. You should then update the `selected:` section in your `config.[env].yaml`
file to install the module.

`NOTE: Using Cognite Functions to run workloads will be limited by the underlying resources
 in the cloud provider functions. Hence processing large volumes of time series & asset data
 will not be optimal in a CDF function since it will time out and fail.`

See [Using Templates](https://developer.cognite.com/sdks/toolkit/templates)
