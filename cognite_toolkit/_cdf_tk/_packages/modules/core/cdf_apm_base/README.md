# cdf_apm_base

The `cdf_apm_base` module manages the basic set of data models needed for
all Asset Performance Management use cases, as well as Infield and Maintain.

The current version of the module is targeted and validated for Infield v2.

## Managed resources

This module manages the following resources:

1. A set of containers built on the system models that are used to store core entities in maintenance.
2. A set of views that offers access to these core entities.
3. A data model that can be used to access the views and organises the relationships between the entities.
4. A space where all these entities are created

## Variables

The following variables are required and defined in this module:

| Variable | Description |
|----------|-------------|
| apm_datamodel_space| The space where this data model should be created. |
| apm_datamodel_version| The version of the data model that should be created. |

> DO NOT CHANGE these variables unless you know what you are doing!

## Usage

Other packages like cdf_infield use this module to create the basic data model before installing more
location and customer specific configurations on top.

You can install this module for other maintenanance and asset performance use cases by just including
the module name under `deploy:` in your `environments.yaml` file.
