# cdf_oid_example_data

## About the module

The module has a basic data set that can be used as example data for many other modules and
purposes. It is used by other packages and modules like cdf_infield_location.

## About the data

This data is from [Open Industrial Data](https://hub.cognite.com/open-industrial-data-211/what-is-open-industrial-data-994).

Below is a snippet from the Cognite Hub website describing the data:

> The data originates from a single compressor on Aker BP’s Valhall oil platform in the North Sea.
> Aker BP selected the first stage compressor on the Valhall because it is a subsystem with
> clearly defined boundaries, rich in time series and maintenance data.
>
> AkerBP uses Cognite Data Fusion to organize the data stream from this compressor. The data set available in Cognite
> Data Fusion includes time series data, maintenance history, and
> Process & Instrumentation Diagrams (P&IDs) for Valhall’s first stage compressor and associated process equipment:
> first stage suction cooler, first stage suction scrubber, first stage
> compressor and first stage discharge coolers. The data is continuously available and free of charge.
>
>By sharing this live stream of industrial data freely, Aker BP and Cognite hope to accelerate innovation within
data-heavy fields. This includes predictive maintenance, condition
> monitoring, and advanced visualization techniques, as well as other new, unexpected applications. Advancement in these
> areas will directly benefit Aker BP’s operations and will also
>improve the health and outlook of the industrial ecosystem on the Norwegian Continental Shelf.

## Managed resources

This module manages the following resources:

1. Datasets for each resource type in the data set named based on the location name (default:oid).
2. A set of Process & Instrumentation diagrams to be uploaded as files.
3. Pre-structured data in RAW for assets, timeseries, workorders, and workitems that are suitable for creating relationships
   between the different resources. These are structured into one database per source system.
4. A set of timeseries (but no datapoints) for the different assets.
5. A transformation that moves assets from the asset RAW table into the classic asset hierarchy.

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
