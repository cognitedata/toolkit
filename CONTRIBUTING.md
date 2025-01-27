# Contributing

## How to contribute

We are always looking for ways to improve the Cognite Toolkit CLI. You can
report bugs and ask questions in [our Cognite Hub group](https://hub.cognite.com/groups/cognite-data-fusion-toolkit-277).

We are also looking for contributions to new modules (content) and the Toolkit codebase that make the configuration of
Cognite Data Fusion easier, faster and more reliable.

## Improving the codebase

If you want to contribute to the codebase, you can do so by creating a new branch and
[opening a pull request](https://github.com/cognitedata/toolkit/compare). Prefix the PR title with the Jira issue
number on the form `[CDF-12345]`. A good PR should include a good description of the change to help the reviewer
understand the nature and context of the change.

### Linting and testing

The Cognite Toolkit CLI and modules have an extensive test and linting battery to ensure quality and speed of development.

See [pyproject.toml](pyproject.toml) for the linting and testing configuration.

See [tests](tests/README.md) for more information on how to run and maintain tests.

The `cdf_` prefixed modules are tested as part of the product development.

### Setting up the local environment

Your local environment needs a working Python installation and a virtual environment. We use `poetry` to manage
the environment and its dependencies.

Install pre-commit hooks by running `poetry run pre-commit install` in the root of the repository.

When developing in vscode, the `cdf-tk-dev.py` file is useful to run the toolkit. This script will set the
environment and paths correctly (to avoid conflicts with the installed cdf package) and also sets the
`SENTRY_ENABLED` environment variable to `false` to avoid sending errors to Sentry.
In .vscode/launch.json you will see a number of examples of debugging configurations that you can use to debug.

### Essential code

- Main app entry point: [cognite_toolkit/_cdf.py](cognite_toolkit/_cdf.py)
- App subcommands: [cognite_toolkit/_cdf_tk/commands](cognite_toolkit/_cdf_tk/commands)
- Resource loaders: [cognite_toolkit/_cdf_tk/loaders](cognite_toolkit/_cdf_tk/loaders)
- Tests: [tests](tests)
- CI/CD: [.github/workflows](.github/workflows)

### Sentry

When you develop the Cognite Toolkit you should avoid sending errors to  `sentry`. You can control `sentry` by setting
the  `environment` variable `SENTRY_ENABLED=false`. This is set automatically when you use the `cdf-tk-dev.py`.

## Contributing in modules

### Module ownership

The official cdf_* modules are owned by the respective teams in Cognite. Any changes to these
will be reviewed by the teams to ensure that nothing breaks. If you open a PR on these modules,
the PR will be reviewed by the team owning the module.

cdf_infield_location is an example of a team-owned module.

### Adding a new module

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

## Releasing

The templates are bundled with the `cdf` tool, so they are released together.
To release a new version of the `cdf` tool and the templates, you need to do the following:

1. Make sure that the CHANGELOG.cdf-tk.md is up to date. The top entry should be `## TBD` only.
2. Go to <https://github.com/cognitedata/toolkit/actions/workflows/prepare-release.yaml> and run the workflow.

   > Use `patch` to bump z in the x.y.z version number, `minor` to bump y, and `major` to bump x.

   This will create a new preparation branch from `main` with final changes and version bumping,
   e.g. `prepare_for_0_1_0b3`. Use `aX` for alpha, `bX` for beta, and `rcX` for
   release candidate:
   - Updates `CHANGELOG.cdf-tk.md` file with a header e.g. `## [0.1.0b3] - 2024-01-12` and review the
      change comments since the previous release. Ensure that the changes are correctly reflected in the
      comments and that the changes can be easily understood. Also verify that any breaking changes
      are clearly marked as such (`**BREAKING**`).
   - Does the same update to `CHANGELOG.templates.md` file.
   - Updates the files with the new version number, this is done with
      the `cdf bump --patch` (or `--minor`, `--major`, `--alpha`, `--beta`) command.
      - `cognite_toolkit/_version.py`
      - `pyproject.toml`
      - `_system.yaml` (multiple)

   - Runs `poetry lock` to update the `poetry.lock` file.
   - Runs `pytest tests` locally to ensure that tests pass.
   - Runs `python module_upgrade/run_check.py` to ensure that the `cdf-tk modules upgrade` command works as expected
      against previous versions. See [Module Upgrade](module_upgrade/README.md) for more information.

3. Get approval to **squash merge** the branch into `main`:
   1. Verify that all Github actions pass.
4. Create a release branch: `release-x.y.z` from `main`:
   1. Create a new tag on the branch with the version number, e.g. `v0.1.0b3`.
   2. Open a PR with the existing `release` branch as base comparing to your new `release-x.y.z` branch.
   3. Get approval and merge (**do not squash**).
   4. Verify that the Github action `release` passes and pushes to PyPi.
5. Create a new release on github.com with the tag and release notes:
   1. Find the tag you created and create the new release.
   2. Copy the release notes from the `CHANGELOG.cdf-tk.md` file, add a `# cdf-tk` header.
   3. Copy then further below the release notes from the `CHANGELOG.templates.md` file, add
      a `# Templates` header.
   4. Remember to mark as pre-release if this is not a final release.
6. Evaluate necessary announcements:
   1. On the Cognite Hub group, create a new post.
   2. As part of product releases, evaluate what to include.
   3. Cognite internal announcements.
