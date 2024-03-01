# cdf_asset_source_model

This module contains an Asset source model that illustrates how to create a customer-extended Asset that
is correctly tied to the global Asset source model. Follow this pattern to ensure that you can
easily upgrade to new versions of the Asset source model.

## Managed resources

This module manages the following resources:

* A space to hold instances ingested from the asset hierarchy.
* A space to hold the model entities for the extended asset.
* A container for extended asset properties.
* A view for the extended asset.
* A data model for the extended asset.
* A transformation that populates the data from the asset hierarchy into the model.

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| model_space           | The space to create the extended asset model in.    |
| instance_space        | The space where instances should be created.        |
| view_asset_version    | The version to use on the extended asset view.    |
| data_model_version    | The name of the data model for the extended asset.  |
| root_asset_external_id| The external id of the root asset in the asset hierarchy.   |

## Usage

The `example_pump_asset_hierarchy` module contains example data that can be used with this module.
That module loads data from RAW into the classic asset hierarchy.

This module creates an extended asset data model and the transformation populates that model with
data from the asset hierarchy.

Next, you may want to look at the example_pump_data_model module that shows how to extend the
data model even further to sup-types of the extended asset.
