# Contributing

## How to contribute

We are always looking for ways to improve the templates and the workflow. You can
[file bugs](https://github.com/cognitedata/toolkit/issues/new/choose) in the repo.

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
5. Update `default.packages.yaml` in cognite_toolkit root with the new module if it is part of a package
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
The scripts are continuously under development to simplify management of configurations, and
we are pushing the functionality into the Python SDK when that makes sense.

## Testing

The `cdf_` prefixed modules should be tested as part of the product development. Our internal
test framework for scenario based testing can be found in the Cognite private big-smoke repository.

The `cdf-tk deploy` script command will clean configurations if you specify `--drop`, so you can
try to apply the configuration multiple times without having to clean up manually. If you want to delete
everything that is governed by your templates, including data ingested into data models, the  `cdf-tk clean`
script command can be used to clean up configurations using the `scripts/delete.py` functions.

See [tests](tests/README.md) for more information on how to run tests.

## Setting up Environment

When you develop `cdf-tk` you should avoid sending errors to  `sentry`. You can control `sentry` by setting
the  `environment` variable `SENTRY_ENABLED=false`. This is set automatically when you use the `cdf-tk-dev.py`.

## Releasing

The templates are bundled with the `cdf-tk` tool, so they are released together.
To release a new version of the `cdf-tk` tool and the templates, you need to do the following:

1. Create a new preparation branch from `main` where you can make the final changes and do version bumping,
   e.g. `prepare_for_0_1_0b3`. Use `aX` for alpha, `bX` for beta, and `rcX` for
   release candidate:
   1. Update `CHANGELOG.cdf-tk.md` file with a header e.g. `## [0.1.0b3] - 2024-01-12` and review the
      change comments since the previous release. Ensure that the changes are correctly reflected in the
      comments and that the changes can be easily understood. Also verify that any breaking changes
      are clearly marked as such (`**BREAKING**`).
   2. Do the same update to `CHANGELOG.templates.md` file.
   3. Update the files with the new version number, this is done with
      the `cdf bump --patch` (or `--minor`, `--major`, `--alpha`, `--beta`) command.
      - `cognite_toolkit/_version.py`
      - `pyproject.toml`
      - `_system.yaml` (multiple)

      You can use the `python bump --minor --alpha` command to bump the version in all files.
   4. Run `poetry lock` to update the `poetry.lock` file.
   5. Run `pytest tests` locally to ensure that tests pass.
   6. Run `python module_upgrade/run_check.py` to ensure that the `cdf-tk modules upgrade` command works as expected.
      against previous versions. See [Module Upgrade](module_upgrade/README.md) for more information.

      if a check fails due to missing package:
      - source .venv/.../bin/activate
      - pip install dependency
      - deactivate
      - run script again

1. Get approval to squash merge the branch into `main`:
   1. Verify that all Github actions pass.
1. Create a release branch: `release-x.y.z` from `main`:
   1. Create a new tag on the branch with the version number, e.g. `v0.1.0b3`.
   2. Open a PR with the existing `release` branch as base comparing to your new `release-x.y.z` branch.
   3. Get approval and merge (do not squash).
   4. Verify that the Github action `release` passes and pushes to PyPi.
1. Create a new release on github.com with the tag and release notes:
   1. Find the tag you created and create the new release.
   2. Copy the release notes from the `CHANGELOG.cdf-tk.md` file, add a `# cdf-tk` header.
   3. Copy then further below the release notes from the `CHANGELOG.templates.md` file, add
      a `# Templates` header.
   4. Remember to mark as pre-release if this is not a final release.
1. Evaluate necessary announcements:
   1. On the Cognite Hub group, create a new post.
   2. As part of product releases, evaluate what to include.
   3. Cognite internal announcements.
