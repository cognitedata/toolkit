# cdf_data_pipeline_files_valhall

This module relies on these example data modules being loaded:
- cdf_oid_example_data being loaded.
- cdf_data_pipeline_asset_valhall

The module creates a simple data pipeline for processing files from the OID example module. OID is used as the examle location name.
Since the pipeline don't load from a source the example data from cdf_oid_example_data is loaded to files and RAW in CDF.
Content of the data pipeline is:



## Managed resources

This module manages the following resources:

1. data set:
   - ID: ds_files_oid
     - Content: Data linage, used Links to used extraction pipelines, transformation and raw tabels

2. extraction pipeline:
   - ID: ep_src_files_oid_fileshare
     - Content: Documentation and configuration example for the file extractor
   - ID: ep_ctx_files_oid_pandid_annotation
     - Content: Documentation and configuration for a CDF function running P&ID contextualization / annotastion (see function form more description)

3. transformations:
   - ID: tr_files_oid_fileshare_file_matadata
     - Content: update of metadata for example file metdata, prepping for contextualization / annotation


## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| default_location| The default location name to use for the data set. We use default oid (Open Industrial Data) |
| source_asset| The name of the source system where assets originate from, default: workmate|
| source_workorder| The name of the source system where the workorders orginate from, default workmate|
| source_files| The name of the source system where the files originate from, default: workmate|
| source_timeseries| The name of the source system where the timeseries originate from, default: pi|

## Usage

If you want to create a project with example data, you can either specify this module in your `environment.yml` file or
you can copy it to `custom_modules`, change the name (remove the cdf_ prefix), and replace the data with your own in the
various sub-directories.




## Managed resources

This module manages the following resources:

1. a group with read-write access (`gp_cicd_all_read_write`) to everything in a CDF project (for `cdf-tk` as an admin tool or
    through a CI/CD pipeline).
2. a group with read-only access `gp_cicd_all_read_only` (for viewing configurations from UI).

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| default_location      | Name of the location, default oid (Open Industrial Data)        |
| source_asset| The name of the source system where assets originate from, default: workmate|
| source_workorder| The name of the source system where the workorders orginate from, default workmate|
| source_timeseries| The name of the source system where the timeseries originate from, default: pi|
| datamodel             | The name of the data model to create, default: apm_simple  |
| space                 | The space where the data model entities should be created, default: apm_simple  |
| datamodel_version     | The version to use when creating this data model. If you do modifications, you can bump up the version. |
| view_Asset_version    | The version to use on the Asset view.         |
| view_WorkOrder_version| The version to use on the Workorder view.         |
| view_WorkItem_version | The version to use on the WorkItem view.         |
| pause_transformations | Whether transformations should be created as paused.        |

> Note! The `source_asset`, `source_workorder`, and `source_timeseries` variables need to match the corresponding
> variables in the cdf_oid_example_data module as this module uses the RAW tables from that module.

## Usage

This module is not meant for production purposes. It is meant as an example to illustrate how you can create
a data model and populate it with data from the Open Industrial Data data set (cdf_oid_example_data).

It can be used standalone, but the transformations will then not be able to run without
modifications.
