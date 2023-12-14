# example_pump_data_model

This module contains the data model and transformation for Lift stations with pumps.
It shows how to extend the system data models for assets to represent custom
properties for lift stations and pumps, as well as how to transform data from a generic
asset space/model.

## Managed resources

This module manages the following resources:

* Two spaces for both the model and instances.
* A container for pump asset properties.
* Two views for pump and liftstation.
* A data model for the pump and liftstation.

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| model_space              | Space for the data models.           |
| instance_space           | Space to store instances.         |
| source_model_space       | Space to find source assets to ingest from.  |
| source_model             | Which data model to use to ingest asset data from.        |
| view_Pump_version        | Version to use on the pump view.      |
| view_LiftStation_version | Version to use on the liftstation view.     |
| data_model_version       | Version to use on the LiftStation data model.      |
| data_model               | The name to use for the LiftStaion data model.        |

## Usage

The `example_pump_asset_hierarchy` module contains example data that can be used with this module.

The `cdf_asset_source_model` model shows how to extend the system data model for assets to represent custom properties,
as well as how to transform data from the classic asset hierarchy into the extended asset model.

Finally, this module shows how to extend the data model even further to sub-types of the extended asset and how to
categorise the assets from the `source_model` found in the `source_model_space`.
