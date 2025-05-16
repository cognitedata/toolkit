# cdf_infield_location

This module contains location specific configurations for Infield. This is the default location.
The support multiple locations, copy this module and modify the configurations. Remember to
rename the module name to e.g. `infield_location_<default_location>`.

## Defaults

The module has been set up to use data from the `examples/cdf_oid_example_data` module. That module
loads data into RAW, and a transformation to move data
into the classic asset hierarchy in CDF.

This module only uses the data from the classic asset hierarchy to populate assets in data models, and
from the RAW tables to make activities from the `workorders` table in RAW.

In [./default.config.yaml](default.config.yaml), you will find default values that are adapted to the
`cdf_oid_example_data` data set.

## app_config

This module creates a configuration instance of Infield by populating the APM_Config data model with a configuration. This
configuraiton ties together groups, spaces, and the root asset externalId for a specific Infield location.

## Auth

The module creates four groups that need four matching groups in the identity provider that the CDF
project is configured with. The groups are:

* Normal role for regular Infield users
* A viewer role for read/only users
* A template administrator role
* A checklist administrator role

The source ids from the groups in the identity provider should be set in [./default.config.yaml](default.config.yaml).

## Data models

There are two spaces created in this module: one space for Infield to store app data and one space for
data from source systems, like assets, activities/work orders etc.

## Transformations

You will find three transformations in this module:

* [Assets hierarchy to data models](./transformations/sync_assets_from_hierarchy_to_apm.sql) - This transformation
  will duplicate the asset hierarchy into the APM data models. It uses the default location source data space to
  store the instances. If you have multiple locations, you need one such transformation per asset hierarchy/location.
  This transformation should run before everything else.
* [Assets hierarchy parent relationships to data models](./transformations/sync_asset_parents_from_hierarchy_to_apm.sql)
   -- This transformation should run after the asset hierarchy has been copied the APM data models as it
   requires the assets to exist.
* [Work orders to activities](./transformations/sync_workorders_to_activities.sql) - This transformation will copy
  work orders from the RAW data model to the APM data model. This transformation relies on the `examples/apm_simple` module
  and the RAW workorders table. In a customer deployment, you will adapt this transformation to populate APM _Activity
  daya model with work orders from the customer's source system.

## Implementation notes

* APM_Activity requires startDate and endDate set to a time range viewed from Infield (for the sample, we hard code these).
* APM_Activity source cannot be null, it must be set to something (don't use APP as this is reserved for Infield).
* APM_Activity rootLocation must exactly match the externalId of the configured root asset.
