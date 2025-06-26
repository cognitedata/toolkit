# Module libraries [ALPHA]

The Toolkit can import modules from external libraries. This is an ALPHA feature.

## Usage

A user can add packages to cdf.toml. At the moment, the Toolkit only loads one library.

```toml
[alpha_flags]
external-libraries = true


[library.package_1]
url = "https://raw.githubusercontent.com/cognitedata/toolkit-data/librarian/builtins.zip"
checksum = "sha256:1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

```

When running `cdf modules init` or `cdf modules add`, the packages will be imported and selectable for the user.

## Publishing a library

The library must be available over https. Authentication is not currently supported.

To publish a library, create a repository that contains one or more downloadable zip files.
The zip file must have the structure and content described below.

The checksum is mandatory. It is used to verify the integrity of the downloaded zip file.
It must be a SHA-256 checksum of the zip file.

### <package_1>.zip

Zip file content must be structured like this:

```shell

<package_1>.zip
├── package.toml
└── module_1
    ├── <module content>
    └── default.config.yaml
└── module_2
    ├── <module content>
    └── default.config.yaml
```

#### packages.toml

The packages.toml in the root folder is required to make the library discoverable by the Toolkit. It allows you to describe
the content of the library and bundle modules into packages. A module can belong to several packages.

The **packages.toml** file should contain the following information:

```toml

[package]
description = "<Description for the end user>"
toolkit-version = ">=0.6.0" # Recommended version of the Toolkit required to use this library
canCherryPick = true # Set to false if the user should not be able to pick individual modules in this package

# Package definition
[packages.quickstart]
title = "Quickstart"
description = "Get started with Cognite Data Fusion in minutes."
canCherryPick = false

# modules that belong to this package
[[packages.quickstart.modules]]
name = "timeseries_and_assets"
description = "Create basic time series and assets."

[[packages.quickstart.modules]]
name = "transformations_and_dms"
description = "Set up simple transformations and data models."
```

#### default.config.yaml

A **default.config.yaml** file must be present in each module folder *if the module contains any value placeholders.*

In other words, if your module has any curly bracket placeholders in the resource file `my.<Kind>.yaml` file,
you must provide a **default.config.yaml** file with those values:

```yaml
#<root folder>/module_1/data_models/my.Space.yaml
space: sp_{{ location }}_instances
name: {{ location_name }}_instances
description: 'Contains instances for {{ location_name }}.'

#<root folder>/module_1/data_models/my.DataSet.yaml
externalId: ds_{{ location }}_functions
name: '{{location_name}} Functions'
description: This dataset contains Functions for the {{location_name}}.

#root folder>/module_1/default.config.yaml
location: <not set>
location_name: <not set>
```

When cdf modules init or cdf modules add is run, the default values from **default.config.yaml** will be automatically
added to **config.dev.yaml** and **config.prod.yaml** so that the user can maintain the correct values.

```yaml

environment:
  project: <cdf project>
  type: dev
  selected:
  - modules/

variables:
  modules:
    module_1:
      location: <not set>
      location_name: <not set>
```

## Verification

To verify that the library is correctly structured:

1. Add your library path ending with the root folder to cdf.toml:

```toml
[libraries.my_library]
url = https://example.com/my-library/my-root-folder
```

1. Run `cdf modules init` or `cdf modules add` to verify that packages and modules are shown correctly
1. Verify that the module content is copied into the local modules/ folder

## Rate limiting

Certain cloud services, like GitHub, may rate limit requests to the library URL. Consider using a CDN or
similar if concurrent requests are expected.
