# Module and Package Documentation

This is the starting point for documentation on modules and packages.

## Packages

## infield and infield_demo

The `infield_demo` packages are used to create a demo environment for infield with sample data.
The `infield` package should be loaded for a customer project that is using Infield.
It loads common Asset Performance Management (APM) modules and Infield specific modules.

See the individual modules for details:

- [cdf_apm_base](#cdf_apm_base)
- [cdf_infield_common](#cdf_infield_common)
- [cdf_infield_location](#cdf_infield_location)

## Common modules

Common modules are designed to used for many different types of CDF configurations. Typically,
they configure core functionality like access groups, identity providers, etc.

### [cdf_auth_readwrite_all](../common/cdf_auth_readwrite_all/README.md)

The `cdf_auth_readwrite_all` module is used to create a set of basic access groups for CI/CD pipeline
and read access to configurations from the UI.

### [cdf_idp_default](../common/cdf_auth_idp_default/README.md)

The `cdf_idp_default` module is used to create a default identity provider for CDF. This identity
provider is a "starter" configuration and the module should be copied to the modules directory
e.g. with the name `idp_config` and modified to set configurations for the customer's identity provider.

## cdf_* modules

### [cdf_apm_base](../modules/cdf_apm_base/README.md)

Basic data models for Asset Performance Management (APM) projects.

### [cdf_infield_common](../modules/cdf_infield_common/README.md)

Common configurations for Infield that are only needed once per CDF project.

### [cdf_infield_location](../modules/cdf_infield_location/README.md)

Per location configurations necessary when setting up a new location for Infield.

## Example modules

The example modules are used to demonstrate how to create modules for specific use cases or
to offer example data. These modules can be copied and used for your own modules, but when they
are updated, changes may be breaking and you cannot expect to be able to migrate an older example
module to a newer one.

### [cdf_apm_simple](../examples/cdf_apm_simple/README.md)

Sample data for APM applications like Infield and Maintain.
