# cdf_data_pipeline_files_valhall

This module relies on these example data modules being loaded:

- cdf_oid_example_data
- cdf_data_pipeline_asset_valhall

The module creates a simple data pipeline for processing files from the OID example module.
The processing here is related to the annotation/contextualization mapping of tags in P&ID documents
to assets in the asset hierarchy.

OID is used as the example location name.
Since the pipeline doesn't load from a source, the example data from cdf_oid_example_data is loaded to files and RAW in CDF.

Required example data in the module `cdf_oid_example_data` is a small data set from
[Open Industrial Data](https://learn.cognite.com/open-industrial-data), the Valhall platform.

## Managed resources

This module manages the following resources:

1. auth:
   - Name: `gp_files_oid_extractor`
     - Content: Authorization group used by the extractor writing data to CDF RAW, files, pipeline and
       update of extraction pipeline run
   - Name: `gp_files_oid_processing`
     - Content: Authorization group used for processing the files, running transformation,
       function (contextualization) and updating files
   - Name: `gp_files_oid_read`
     - Content: Authorization group used by the users reading files and annotations

2. data set:
   - ID: `ds_files_oid`
     - Content: Data lineage, used Links to used extraction pipelines, transformation and raw tables

3. extraction pipeline:
   - ID: `ep_src_files_oid_fileshare`
     - Content: Documentation and configuration example for the file extractor
   - ID: `ep_ctx_files_oid_pandid_annotation`
     - Content: Documentation and configuration for a CDF function running P&ID contextualization / annotation
       (see function form more description)

4. transformations:
   - ID: `tr_files_oid_fileshare_file_metadata`
     - Content: update of metadata for example file metadata, prepping for contextualization / annotation
     - NOTE: the transformation must run before the contextualization function. Without the transformation the
       function will not be able to find the files to contextualize.

5. function:
   - ID: `fu_context_files_oid_fileshare_annotation`
     - Content: Extracts all tags in P&ID that matches tags from Asset Hierarchy and creates CDF annotations used for linking
       found objects in document to other resource types in CDF

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

You should copy and rename an example module into the `custom_modules` directory (remove any `cdf_` prefixes) and make
your own modifications. You should then update the `selected_modules_and_packages:` section in your `config.[env].yaml`
file to install the module.

`NOTE: Using Cognite Functions to run workloads will be limited by the underlying resources in the cloud provider functions.
Hence processing many P&ID documents will not be optimal in a CDF function since it will time out and fail.`

See [Using Templates](https://developer.cognite.com/sdks/toolkit/templates)
