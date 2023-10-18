# INTERNAL NOTES

## Initial starting point

The initial starting point for this repository was the data-model-examples repository, and
the apm_simple configuration is used as a skeleton for an example model so we can start iterating.

## Workflow

As this is a public repository, the work tasks should be managed in Github issues, while
we use [Jira](https://cognitedata.atlassian.net/jira/software/c/projects/CDF/boards/882) to
manage internal work tasks.
The global.yaml file in this repo should be updated to configure groups of modules that
can be deployed (as specified in local.yaml).
Use the `cdf_` prefix for modules that are official Cognite product modules.
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

All the configurations should be kept in YAML and in a format that is compatible with the CDF API, thus
using snake_case, camelCase is not supported (e.g. external_id and not externalId).
The configuration files should be loaded directly into the Python SDK's support data classes for direct
use towards the CDF API. No client side schema validation should be done to ensure that you can immediately
add a yaml configuration property without upcoming anything else than the version of the Python SDK.

## Tooling and scripts/ directory

The ./scripts directory is originally from <https://github.com/cognitedata/data-model-examples>
repository, but substantially refactored to support a CI/CD aka `CDF-as-code`` workflow.

We want to add client-side logic/validation as part of the deployment process, e.g. validation
of data models, transformations, contextualizations, etc to ensure integrity and proper
functioning configuration.

The future intent is to establish a `v1/<project>/config` CDF service that will take over this
validation done by code in the ./scripts directory. We also want to push as much generic logic
into the Python SDK as possible.

> NOTE!! The scripts currently support raw, data models, time series,  groups, and transformations.
> It also has some support for loading of data that may be used as example data for CDF projects. However,
> to the extent possible, this repository should not contain data, only goverend configurations.
> There is also a dump.py file with functions to dump configurations from CDF into yaml files, thus
> supporting the workflow from UI-based iteration on configurations to CI/CD-based governed configurations.

## Testing

The `cdf_` prefixed modules should be tested as part of the product development. Our internal
test framework for scenario based testing can be found in the Cognite private big-smoke repository.

> TODO Define how to make sure that modules get tested in big-smoke.

The `deploy.py` script will automatically clean configurations before trying to load, so you can
try to apply the configuration multiple times without having to clean up manually. There is also
a skeleton for a `clean.py` script that will be used to clean up configurations using the scripts/delete.py
functions.

