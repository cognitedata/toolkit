# Contributing

## How to contribute

We are always looking for ways to improve the templates and the workflow. You can
[file bugs](https://github.com/cognitedata/cdf-project-templates/issues/new/choose) in the repo.

We are also looking for contributions to new modules, especially example modules can be very
useful for others. Please open a PR with your suggested changes or propose a functionality
by creating an issue.

## Module ownership

The official cdf_* modules are owned by the respective teams in Cognite. Any changes to these
will be reviewed by the teams to ensure that nothing breaks. If you open a PR on these modules,
the PR will be reviewed by the team owning the module.

cdf_infield_location is an example of a team-owned module.

## Adding a new module

Adding a new module consists of the following steps:

1. Determine where to put it (core, common, modules, examples, or experimental).
2. Create a new directory for the module with sub-directories per configuration type the module needs. See the
   [YAML reference documentation](https://developer.cognite.com/sdks/toolkit/references/configs).
3. Add a `default.config.yaml` file to the module root directory if you have variables in the templates.
4. Add a `README.md` file to the module root directory with a description of the module and variables.
5. Update `default.packages.yaml`  in cognite_toolkit root with the new module if it is part of a package
6. If this is an official module, add a description of the module in the
   [module and package documentation](https://developer.cognite.com/sdks/toolkit/references/module_reference).

> If you are not a Cognite employee and would like to contribute a module, please open an issue, so we can
> get in touch with you.

Each module should be as standalone as possible, but they can be dependent on other modules.
If you need to deploy a data model as a foundational
element for both transformations and applications to work, you may add a module with the data model.
However, a better module would be one that includes all the elements needed to get data from the
source system, through RAW (if necessary), into a source data model, and then transformed by one or
more transformations into a domain data model. The solution data models can then be a separate module
that relies on the ingestion module.

Please take care to think about the best grouping of modules to make it easy to deploy and maintain.
We are aiming at standardizing as much as possible, so we do not optimize for customer-specific
changes and naming conventions except where we design to support it.

> NOTE! Customer-specific projects should be able to use these templates directly, and also adopt
> new changes from this repository as they are released.
> Configurations that contain defaults that are meant to be changed by the customer, e.g. mapping
> of properties from source systems to CDF, should be contained in separate modules.

## Data formats

All the configurations should be kept in camelCase YAML and in a format that is compatible with the CDF API.
The configuration files are loaded directly into the Python SDK's support data classes for
use towards the CDF API. Client side schema validation should be done in the Python SDK and not in `cdf-tk`
to ensure that you can immediately
add a yaml configuration property without upcoming anything else than the version of the Python SDK.

> NOTE!! As of now, any non-recognised properties will just be ignored by the Python SDK. If you don't
> get the desired configuration deployed, check your spelling.

The scripts currently support many resources like raw, data models, time series, groups, and transformations.
It also has some support for loading of data that may be used as example data for CDF projects. However,
as a general rule, templates should contain governed configurations necessary to set up ingest, data pipelines,
and contextualisations, but not the actual data itself.

Of course, where data population of e.g. data model is part of the configuration, that is fine.
The scripts are continuosly under development to simplify management of configurations, and
we are pushing the functionality into the Python SDK when that makes sense.

## Testing

The `cdf_` prefixed modules should be tested as part of the product development. Our internal
test framework for scenario based testing can be found in the Cognite private big-smoke repository.

The `cdf-tk deploy` script command will clean configurations if you specify `--drop`, so you can
try to apply the configuration multiple times without having to clean up manually. If you want to delete
everything that is governed by your templates, including data ingested into data models, the  `cdf-tk clean`
script command can be used to clean up configurations using the scripts/delete.py functions.
