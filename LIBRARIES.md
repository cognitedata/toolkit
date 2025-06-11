# Module libraries

The Toolkit can import modules from external libraries.

## Usage

A user can add packages to cdf.toml:

```toml
[library.package_1]
url = https://example.com/my-library/<package_1>.zip

[library.package_2]
url = https://github.com/cognitedata/library/archive/refs/tags/0.0.1.zip
```

When running `cdf modules init` or `cdf modules add`, the packages will be imported and selectable for the user.

## Publishing a library

The library must be available over https. Authentication is not currently supported.

To publish a library, create a repository that contains one or more downloadable zip files.
The zip file must have the structure and content described below.

### <package_1>.zip

Zip file content must be structured like this:

```shell

<package_1>.zip
├── package.toml
└── module_1
    ├── <module content>
    ├── default.config.yaml
    └── module.toml
└── module_2
    ├── <module content>
    ├── default.config.yaml
    └── module.toml
```

#### package.toml

The package.toml in the root folder is required to make the library discoverable by the Toolkit. It allows you to describe
the content of the library and bundle modules into packages. A module can belong to several packages.

The **packages.toml** file should contain the following information:

```toml

[package]
description = "<Description for the end user>"
toolkit-version = ">=0.6.0" # Recommended version of the Toolkit required to use this library
canCherryPick = true # Set to false if the user should not be able to pick individual modules in this package

[packages.quickstart]
title = "Quickstart"
description = "Get started with Cognite Data Fusion in minutes."
canCherryPick = false

[[packages.quickstart.modules]]
name = "timeseries_and_assets"
description = "Create basic time series and assets."

[[packages.quickstart.modules]]
name = "transformations_and_dms"
description = "Set up simple transformations and data models."
```

#### module.toml

A **module.toml** file must be present in each module folder. It should contain the following information:

```toml
[module]
title = "Module 1"
is_selected_by_default = false # Set to true if the module should be selected by default when the user runs cdf modules init
 
[packages]
tags = [
    "my_package_1", "my_package_3" # use tags to indicate which packages the module should be available in
]
```

#### default.config.yaml

A **default.config.yaml** file must be present in each module folder if the module contains any value placeholders.

In other words, if your module has any placeholders in the resource file `my.<Kind>.yaml` file,
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
type = https
url = https://example.com/my-library/my-root-folder
```

1. Run `cdf modules init` or `cdf modules add` to verify that packages and modules are shown correctly
1. Verify that the module content is copied into the local modules/ folder

## Versioning

When importing, the Toolkit will check the `toolkit-version` in the `package.toml` file and
issue a warning if it is incompatible with the current Toolkit version.

As a maintainer, it is possible to maintain multiple versions of the library by creating separate folders in the repository:

```yaml
#<root folder>/v1/package.toml
[packages]
title = "<descriptive title for the library>"
toolkit-version = "<0.6"

#<root folder>/v2/package.toml
[packages]
title = "<descriptive title for the library>"
toolkit-version = ">=0.6"
```

## Rate limiting

Certain cloud services, like GitHub, may rate limit requests to the library URL. Consider using a CDN or
similar if concurrent requests are expected.
