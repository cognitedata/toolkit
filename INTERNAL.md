# INTERNAL NOTES

## Initial starting point

The initial starting point for this repository is the data-model-examples repository, and
the apm_simple configuration is used as a skeleton for an example model so we can start iterating.

## Workflow

As this is a public repository, the work tasks should be managed in Github issues.
The global.yaml file in this repo should be updated to configure groups of modules that
can be deployed. Use the `cdf_` prefix for modules that are official Cognite product modules.
These should be validated by product teams and tested as part of product development.

### Adding a new module

Each module should be as standalone as possible, but they can be dependent on either modules
in ./common or other modules in ./modules. If you need to deploy a data model as a foundational
element for both transformations and applications to work, you may add a module with the data model.
However, a better module would be one that includes all the elements needed to get data from the
source system, through RAW (if necessary), into a source data model, and then transformed by one or
more transformations into a domain data model. The solution data models can then be a separate module
that relies on the ingestion module.

Please take care to think about the best grouping of modules to make it easy to deploy and maintain.
We are aiming at standardizing as much as possible, so we do not optimize for project-specific
changes and naming conventions except where we design for it.

> NOTE! Customer-specific projects should be able to use these templates directly, and also adopt
> new changes from this repository as they are released.
> Configurations that contain defaults that are meant to be changed by the customer, e.g. mapping
> of properties from source systems to CDF, should be contained in separate modules.

## Data formats

All the configurations should be kept in JSON and in a format that is compatible with the CDF API.
For transformations, we will support importing yaml files for backwards compatibility, but the storage
of the configuration will be in JSON format.

## Tooling and utils/ directory

The ./utils directory is initally copied from <https://github.com/cognitedata/data-model-examples>
repository as it has initial Python logic for dumping and loading data to and from the APIs.
There is no need to keep the two repos in sync, and we can iterate on the tooling in ./utils
to make it easier to work with the templates.

The intent is to establish a `v1/<project>/config` CDF service that will take over most
of the heavy-lifting done by code in the ./utils directory. Both this template repo and
data-model-examples repo can then be updated to use the new service and thus become
simpler and easier to maintain.

> NOTE!! The utils only support raw, data models, time series, (partial) groups, and transformations. 
> It also
> has some support for loading of data that may be used as example data for CDF projects. However,
> to the extent possible, this repository should not contain data, only goverend configurations.

## Testing

The `cdf_` prefixed modules should be tested as part of the product development. Our internal
test framework for scenario based testing can be found in the Cognite private big-smoke repository.

> TODO Define how to make sure that modules get tested in big-smoke.

The `deploy.py` script will automatically clean configurations before trying to load, so you can
try to apply the configuration multiple times without having to clean up manually. There is also
a skeleton for a `clean.py` script, but the current delete functions in utils do not accept a directory
as input (like the load_* functions), so the directory is hardcoded (not a lot of work though).

> Also note that the utils/ directory has several datamodel delete functions (like cleaning out an
> entire project), as well as several useful dump functions that creates json for datamodels and
> transformations.
