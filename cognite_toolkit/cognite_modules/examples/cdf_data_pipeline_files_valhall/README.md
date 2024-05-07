# `cdf_data_pipeline_files_valhall`

This module relies on these example data modules being loaded:

- `cdf_oid_example_data`
- `cdf_data_pipeline_asset_valhall`

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
     - Content: Data lineage, used Links to used extraction pipelines, transformation, and raw tables

3. extraction pipeline:
   - ID: `ep_src_files_oid_fileshare`
     - Content: Documentation and configuration example for the file extractor
   - ID: `ep_ctx_files_oid_pandid_annotation`
     - Content: Documentation and configuration for a CDF function running P&ID contextualization/annotation
       (see function for more description)

4. transformations:
   - ID: `tr_files_oid_fileshare_file_metadata`
     - Content: update of metadata for example file metadata, prepping for contextualization/annotation
     - NOTE: the transformation must run before the contextualization function. Without the transformation the
       function will not be able to find the files to contextualize.

5. function:
   - ID: `fn_context_files_oid_fileshare_annotation`
     - Content: Extracts all tags in P&ID that match tags from Asset Hierarchy and create CDF annotations used for linking
       found objects in the document to other resource types in CDF
   - ID: `fn_context_files_oid_fileshare_annotation`
     - Content: Function used to schedule & start workflow : `wf_oid_files_annotation`  Loops and waits for
       feedback within the run time limitations for the function

6. workflow
   - ID: `wf_oid_files_annotation`
     - Content: Start Transformation:  `tr_files_oid_fileshare_file_metadata` and then
       Function: `fn_context_files_oid_fileshare_annotation`

### Illustration of the files data pipeline

![image](https://github.com/cognitedata/toolkit/assets/31886431/3722b1ad-ecb6-4251-84d0-ae31ee63e676)

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| location_name | The location for your data, the name used in all resource types related to the data pipeline. We use oid (Open Industrial Data) |
| source_name | The name of the source making it possible to identify where the data originates from, ex: 'workmate', 'sap', 'oracle',..|
| files_dataset | The name of the data set used for files in this example, must correspond to the name used in the example data|
| pause_transformations | Whether transformations should be created as paused.|
| files_raw_input_db | CDF RAW DB name used for files metadata, must correspond to name used in example data|
| files_raw_input_table | CDF RAW DB name used for files metadata, must correspond to name used in example data|
| files_location_extractor_group_source_id | Object/ Source ID for security group in IdP. Used to run integration/extractor|
| files_location_processing_group_source_id | Object/ Source ID for security group in IdP. Used to run CDF processing/contextualization|
| files_location_read_group_source_id | Object/ Source ID for security group in IdP. Used to read file data|

## Usage

You should copy and rename an example module into the `custom_modules` directory (remove any `cdf_` prefixes) and make
your own modifications. You should then update the `selected_modules_and_packages:` section in your `config.[env].yaml`
file to install the module.

### Notes

#### Running functions locally

First of all, let it be said: _The toolkit repository is not an ideal environment for active code development_. A
suggested solution from the toolkit developers is to work with your functions elsewhere and just copy in the files
whenever testing and development is done.

With that disclaimer out of the way, let's have a look at how you may run locally:

To run `fn_context_files_oid_fileshare_annotation`, simply call the `handler.py` normally from the root folder of
the toolkit - or any subsequent folder, as long as you don't enter into the "package" itself, i.e.
`fn_context_files_oid_fileshare_annotation`:

```txt
cognite_toolkit/
  cognite_modules/
    examples/
      cdf_data_pipeline_files_valhall/
        functions/
```

Assuming you have navigated to `functions`, you would do:

```bash
poetry run python fn_context_files_oid_fileshare_annotation/handler.py
```

This works because a special `run_locally` method has been added (imports also magically work). A list of
required environment variables, mostly for authentication towards CDF will be raised if not set correctly
(we won't list them here in case of changes).

#### Cognite Function runtime

Using Cognite Functions to run workloads will be limited by the underlying resources in the cloud provider
functions. Hence processing many P&ID documents will not be optimal in a CDF function since it will time
out and fail. One solution for this is to do the initial one-time job locally and let the function deal
with all new and updated files.

See also: [Using Templates](https://developer.cognite.com/sdks/toolkit/templates)
