# cdf_data_pipeline_asset_valhall

This module relies on these example data modules being loaded:

- cdf_oid_example_data

The module creates a simple data pipeline for processing assets from the OID example module.
OID is used as the examle location name.
Since the pipeline doesn't load from a source, the example data from cdf_oid_example_data is loaded to files and RAW in CDF.

Requred example data in the module `cdf_oid_example_data` is a small data set from [Open Industrial
Data](https://learn.cognite.com/open-industrial-data), the Valhall platform.

## Managed resources

This module manages the following resources:

1. data set:
   - ID: `ds_asset_oid`
     - Content: Data lineage, used Links to used extraction pipelines, transformation and raw tabels

2. extraction pipeline:
   - ID: `ep_src_asset_oid_workmate`
     - Content: Documentation and configuration example for the DB extractor

3. transformations:
   - ID: `tr_asset_oid_workmate_asset_hierarchy`
     - Content: Creation of asset hierarchy based on input asset data from OID

### Illustration of the asset data pipeline

![image](https://github.com/cognitedata/toolkit/assets/31886431/ba534b90-cc8f-4825-9692-d44dad58da6e)

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| location_name | The location for your data, name used in all resource type related to data pipeline. We use oid (Open Industrial Data) |
| source_name | The name of the source making it possible to identify where the data originates from, ex: 'workmate', 'sap', 'oracle',..|
| asset_dataset | The name of data set used for assets in this example, must correspond to name used in example data|
| pause_transformations | Whether transformations should be created as paused.        |
| asset_raw_input_db | CDF RAW DB name used for assets input data, must correspond to name used in example data|
| asset_raw_input_table | CDF RAW DB name used for asset data, must correspond to name used in example data|
| asset_location_extractor_group_source_id | Object/ Source ID for security group in IdP. Used to run integration/extractor|
| asset_location_processing_group_source_id | Object/ Source ID for security group in IdP. Used to run CDF processing/contextualization|
| asset_location_read_group_source_id | Object/ Source ID for security group in IdP. Used to read asset data|

## Usage

If you want to create a project with example data, you can either specify this module in your `environment.yml` file or
you can copy it to `custom_modules`, change the name (remove the cdf_ prefix), and replace the data with your own in the
various sub-directories.

See [Using Templates](https://developer.cognite.com/sdks/toolkit/templates)
