# cdf_apm_simple_data_model

This module relies on cdf_oid_example_data being loaded.
The module creates a simple example Asset Performance Management data model and
adds the necessary transformations to populate the model with data from the
Open Industrial Data data in cdf_oid_example_data.

## Managed resources

This module manages the following resources:

1. a group with read-write access (`gp_cicd_all_read_write`) to everything in a CDF project (for `cdf-tk` as an admin
   tool or through a CI/CD pipeline).
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
