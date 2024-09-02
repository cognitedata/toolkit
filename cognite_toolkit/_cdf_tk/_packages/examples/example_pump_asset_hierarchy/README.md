# example_pump_asset_hierarchy

This module contains an example of source data for pumps and their associated assets with a transformation that moves
the data into the CDF classic asset hierarchy.

## About the data

The dataset is from [Collections Pump](https://data.bendoregon.gov/maps/collections-pump).

From the source:

```txt
 The purpose of this dataset is to show the line configurations associated with the pumps operating at the lift stations
 around the City's Collection system.
```

This is the basis for the practical guide to data modeling in CDF found in the [Cognite Documentation](https://docs.cognite.com/cdf/dm/dm_guides/dm_create_asset_hierarchy).

## Managed resources

This module manages the following resources:

* A dataset to use when ingesting data into the asset hierarchy.
* A RAW table with the pump data.
* A transformation that moves the pump data into the asset hierarchy.

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| raw_db   | The name of the RAW database to use, default: `pump_assets` |
| data_set | The name of the dataset to use when ingesting into the asset hierarchy, default: `src:lift_pump_stations` |

## Usage

This module can be used standalone, but it is meant as example data to be used with the `example_pump_data_model` module.
