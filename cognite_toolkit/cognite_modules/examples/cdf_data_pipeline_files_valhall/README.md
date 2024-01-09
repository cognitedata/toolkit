# cdf_data_pipeline_files_valhall

This module relies on these example data modules being loaded:

- cdf_oid_example_data
- cdf_data_pipeline_asset_valhall

The module creates a simple data pipeline for processing files from the OID example module.
The processing here is related to the annotation/contextualization mapping of tags in P&ID documents to assets in the asset hierarchy.
OID is used as the examle location name.
Since the pipeline doesn't load from a source, the example data from cdf_oid_example_data is loaded to files and RAW in CDF.

Required example data in the module `cdf_oid_example_data` is a small data set from [Open Industrial
Data](https://learn.cognite.com/open-industrial-data), the Valhall platform.

## Managed resources

This module manages the following resources:

1. data set:
   - ID: `ds_files_oid`
     - Content: Data lineage, used Links to used extraction pipelines, transformation and raw tabels

2. extraction pipeline:
   - ID: `ep_src_files_oid_fileshare`
     - Content: Documentation and configuration example for the file extractor
   - ID: `ep_ctx_files_oid_pandid_annotation`
     - Content: Documentation and configuration for a CDF function running P&ID contextualization / annotastion (see function form more description)

3. transformations:
   - ID: `tr_files_oid_fileshare_file_matadata`
     - Content: update of metadata for example file metdata, prepping for contextualization / annotation 

4. function:
   - ID: `fu_context_files_oid_fileshare_annotation`
     - Content: Extracts all tags in P&ID that matches tags from Asset Hierarchy and creates CDF annotations used for linking found objects in document to other resource types in CDF

### Illustration of the files data pipeline

![image](https://github.com/cognitedata/cdf-project-templates/assets/31886431/32c5d53f-5fdb-44a8-9362-35e8152b83e3)

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| location_name | The location for your data, name used in all resource type related to data pipeline. We use oid (Open Industrial Data) |
| source_name | The name of the source making it possible to identify where the data originates from, ex: 'workmate', 'sap', 'oracle',..|
| files_dataset | The name of data set used for files in this example, must correspond to name used in example data|
| pause_transformations | Whether transformations should be created as paused.        |
| files_raw_input_db | CDF RAW DB name used for files metadata, must correspond to name used in example data|
| files_raw_input_table | CDF RAW DB name used for files metadata, must correspond to name used in example data|
| files_location_extractor_group_source_id | Object/ Source ID for security group in IdP. Used to run integration/extractor|
| files_location_processing_group_source_id | Object/ Source ID for security group in IdP. Used to run CDF processing/contextualization|
| files_location_read_group_source_id | Object/ Source ID for security group in IdP. Used to read file data|

## Usage

If you want to create a project with example data, you can either specify this module in your `environment.yml` file or
you can copy it to `custom_modules`, change the name (remove the cdf_ prefix), and replace the data with your own in the
various sub-directories.

See [Using Templates](https://developer.cognite.com/sdks/toolkit/templates)
