# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Changes are grouped as follows:

- `Added` for new features.
- `Changed` for changes in existing functionality.
- `Deprecated` for soon-to-be removed features.
- `Improved` for transparent changes, e.g. better performance.
- `Removed` for now removed features.
- `Fixed` for any bug fixes.
- `Security` in case of vulnerabilities.

## TBD

### Changed

- In the `cdf-tk build` command, if the `Function` code cannot be imported, the Toolkit will now give a
  warning instead of an `ToolkitValidationError`. This is to allow you to deploy code developed in a
  different environment than the environment used to run Toolkit.

## [0.2.15] - 2024-07-22

### Added

- [Feature Preview] Support for uploading `3DModel` resource to CDF. Note this is the metadata about a 3D model
  Turn on the feature by running `cdf-tk features set model-3d --enable`.

### Fixed

- Running `cdf-tk deploy` after a failed build would raise an incorrect `ToolkitNotADirectoryError`,
  instead of a `ToolkitFileNotFoundError` for the `_build_enviroment.yaml` file. This is now fixed.
- When running `cdf-tk deploy` with `Functions` that have not explicitly set `cpu`, `memory`, or `runtime`,
  would always be classified as changed. This is now fixed.
- [Feature Preview] When dumping assets to `csv`, headers are no longer repeated for each 1000 asset.
- [Feature Preview] When dumping assets to `parquet`, you can now dump more than 1000 assets without
  getting the error `TypeError: write() got an unexpected keyword argument 'mode'`.
- [Feature Preview] When dumping assets to `parquet/csv`, the Toolkit now keeps all asset in memory until it finds
  all unique metadata keys. This is to ensure that header is correct in the resulting `parquet/csv` file.
- In the `config.[env].yaml`, the `name` parameter in the `environment` section is no longer required.
  This was supposed to be remove in `0.2.0a4`.
- If you run `cdf-tk build --env dev`, and then `cdf-tk deploy -env prod` the Toolkit will
  now raise a `ToolkitEnvError`.
- If you run `cdf-tk build`, the Toolkit will no longer complain about resources that exist in
  CDF but not in the build directory (given that the Toolkit has access to CDF).
- If you deploy a data model that already exists in CDF, the API will silently fail to update the data model if
  there are any changes to the views in the data model. The Toolkit will now verify that the update of data models
  was successful and raise an error if it was not.

### Changed

- When running `cdf-tk deploy` for a function the Toolkit checked that it could import the function code
  before deploying the function. This is now removed. The reason is that the toolkit is often run in a
  different Python environment than the function code. This made this check unnecessarily restrictive
  as it would fail even though the function code was correct due to missing dependencies.
- [Feature Preview] Instead of using `functionExternalID`+`cron` expression to identify a function schedule,
  the Toolkit now uses `functionExternalID`+`name`. This is to avoid the Toolkit to create multiple schedules
  for the same function if the cron expression is changed and allow to have multiple schedules with the same
  cron expression for the same function. To enable this feature, run `cdf-tk features set fun-schedule --enable`.

## [0.2.14] - 2024-07-15

### Fixed

- If a container with a direct relation property with a required constraint on another container, the `cdf-tk build`
  would not give a warning if the required container was missing. This is now fixed.
- [Feature Preview] In the feature preview, `robotics`, the properties `inputSchema` or `dataHandlingSchema`
  in `RobotCapability` and `DataPostProcessing` were not updated correctly. This is now fixed.
- When running `cdf-tk build`, with a `Node` resource. You would get a `MissingRequiredIdentifierWarning` even though
  the `Node` had a `space`/`externalId`. This is now fixed.
- In the `cdf-tk deploy/clean` command, the `-i` short flag was used for `--interactive` and `--include` at the same time.
  This is now fixed, and the `-i` flag is only used for `--interactive`.
- Require `cognite-sdk>=7.54.1`, this version fixed a bug in the `cognite-sdk` that caused the `cdf-tk` to raise
  an `CogniteAPIError` when deploying or cleaning more than 10 `functions`.

## [0.2.13] - 2024-07-10

### Fixed

- [Feature Preview] In the feature preview, `cdf-tk import transformation-cli`, the Toolkit would fail to convert
  manifest with `destination: <string>` correctly. This is now fixed.
- On Windows, when reading files from disk, the Toolkit could raise an `UnicodeDecodeError`. This is now fixed.
- [Feature Preview] In the feature preview, `robotics`, if you tried to update a set of resources in which some
  were existing and others not, the Toolkit would create the new resources. This is now fixed.

## [0.2.12] - 2024-07-08

### Added

- [Feature Preview] Robotic support.

## [0.2.11] - 2024-07-05

### Fixed

- When running `cdf-tk build`, if you had two files non-YAML files named the same in different modules, or subdirectories
  in the same module, the Toolkit would overwrite the first file with the second file. This is now fixed.

## [0.2.10] - 2024-07-03

### Fixed

- When running `cdf-tk build`, if you use subdirectories in a resource directories, and have two resources with the
  same file name, the Toolkit would overwrite the first resource with the second resource. This is now fixed. For
  example, if you have `my_module/transformation/subdirectory1/my_transformation.Transformation.yaml` and
  `my_module/transformation/subdirectory2/my_transformation.Transformation.yaml`, the Toolkit would only build the
  second resource.

## [0.2.9] - 2024-07-02

### Changed

- Tracking usage of Toolkit commands.

## [0.2.8] - 2024-07-01

### Added

- [Feature Preview] Option to turn off semantic naming checks for resources. Turn on the feature by running
  `cdf-tk features set no-naming --enable`.

### Fixed

- When running `cdf-tk run function --local`, the toolkit would raise an `ToolkitValidationError`. This is now fixed.
- When running `cdf-tk deploy --dry-run`, if any resource was referencing a `DataSet`, `SecurityCategory`,
  or `ExtractionPipeline`, it would incorrectly be classified as changed. This is now fixed. This applied to
  `ExtractionPipeline`, `FileMetadata`, `Function`, `Group`, `Label`, `TimeSeries`, and `Transformation` resources.

### Changed

- Function configurations for multiple functions can now be in multiple files in the function directory. Before
  all configurations had to be listed in the same YAML file.

## [0.2.7] - 2024-06-28

### Fixed

- Function schedule for functions with a `:` in the external id would raise an `ValueError`.
  This is now fixed.
- Transformation notifications for transformations with a `:` in the external id would raise an `ValueError`.
  This is now fixed.
- When running `cdf-tk deploy`, you would get warnings about unrelated resources that were not part of the deployment.
  This is now fixed.
- The `transformations/notifications` endpoint was giving `500` errors for requests to non-exising transformations.
  This is now handled by the toolkit and will not raise an error.
- When doing variable replacement in a `sql` such as `dataset_id('{{dataset_external_id}}')`, the toolkit would
  remove the quotes. This is now fixed.

## [0.2.6] - 2024-06-26

### Improved

- The `--verbose` flag is now moved to the end of the command. For example, instead of `cdf-tk --verbose build`,
  you should now write `cdf-tk build --verbose`. The old syntax is still supported but will raise a deprecation warning.
- When running `cdf-tk deploy --verbose` you will now get a detailed output for each resource that has changed
  (or will change if you use --dry-run).
- Allow values `test` and `qa` as `type` in the `config.[env].yaml` file.

### Fixed

- When running `cdf-tk build` with `Views` with custom filters, the Toolkit would likely give a `UnusedParameterWarning`.
  This is now fixed by not validating the details of `View.filters`. The motivation is that `View.filters` is a complex
  structure, and it is likely that you will get a false warning. The users that starts to use `View.filters` are
  expected to know what they are doing.
- If you run `cdf-tk deploy` and you had a child view that overrides a property from a parent view, the Toolkit would
  log it as changed even though it was not. This is now fixed.

## [0.2.5] - 2024-06-25

### Fixed

- When running `cdf-tk build`, with `RAW` tables in the selected modules, the Toolkit would always warn that the
  tables were missing, even though they were present. This is now fixed.
- When running `cdf-tk init --upgrade <YOUR PROJECT>` form version `0.1.4` the user would get a
  `ERROR (ToolkitMigrationError): Failed to find migration from version 0.1.4.`. This is now fixed.
- When running `cdf-tk build`, the Toolkit would give you warning when referencing a system `Space`, `View`, `Container`
  or `DataModel`. This is now fixed.
- [Feature Preview] When running `cdf-tk import transformation-cli` on a manifest with a query file that
  is separated from the manifest, the toolkit would raise a `FileNotFoundError`. This is now fixed.

## [0.2.4] - 2024-06-24

### Added

- [Feature Preview] Support for resource type `Asset` in the `assets` folder. Turn on the feature by running
  `cdf-tk features set assets --enable`.

### Improved

- When running `cdf-tk build` and the selected modules is missing, the user will now get a hint about
  how to fix the issue.
- When running `cdf-tk build` and a module contains non-resource directories, the user will now get a warning
  that the directory is not a resource directory.

### Fixed

- The data type of variables `config.[env].yaml` file is now preserved. For example, if you had `my_variable: "123"`,
  then the `cdf-tk build`  would build the resource file with the number instead of the string, `my_variable: 123`.
  This is now fixed.
- File configurations given as a list/array, lost the `dataSetExternalId` in the `cdf-tk deploy` command.
  This is now fixed.
- Added missing required dependency `packaging` to `cognite-toolkit`.

## [0.2.3] - 2024-06-20

### Improved

- When running `cdf-tk build` and missing `CDF_PROJECT` environment variable, the user will now get a more informative
  error message.

### Fixed

- The variable `type` in the `environment` section of the `config.[env].yaml` now raises an error if it is not
  set to `dev`, `staging`, or `prod`.

### Added

- The preview feature `IMPORT_CMD` added. This enables you to import a `transformation-cli` manifest into
  resource configuration files compatible with the `cognite-toolkit`. Activate by running
  `cdf-tk features set IMPORT_CMD --enable`, and deactivate by running `cdf-tk features set IMPORT_CMD --disable`.
  Run `cdf-tk import transformation-cli --help` for more information about the import command.

## [0.2.2] - 2024-06-18

### Improved

- The command line messages have been improved to be more informative and user-friendly when running
  `cdf-tk auth verify`.
- The `variables` section in `config.[env].yaml` is now optional.
- In `cdf-tk build`, more informative error message when a variable is unresolved in a resource file.

### Fixed

- In the `cdf-tk auth verify` command, if the flag `--interactive` was set, the `--update-group` and `create-group`
  flags were not ignored. This is now fixed.
- In the `cdf-tk auth verify` command, if there was no `.env` or `--cluster` and `--project` flags, the toolkit
  would raise an `AuthentciationError`, instead of prompting the user for cluster and project. This is now fixed.
- In the `cdf-tk auth verify` command, the if function service was not activated, the toolkit will
  now activate it.
- When running `cdf-tk build`, and a resource file was missing its identifier, for example, `externalId` for a
  dataset, an error such as `AttributeError: 'NoneType' object has no attribute 'split'` was raised. This is now fixed.

## [0.2.1] - 2024-06-17

### Improved

- When running `cdf-tk auth verify`, if the client does not have access to the `CDF_PROJECT` the user will now get
  a more informative error message.
- When running `cdf-tk auth verify` and missing the `FunctionAcl(READ)` capability, the user will now get a more
  informative error message when checking the function service status

### Added

- Preview feature `MODULES_CMD` to allow interactive init and automatic upgrade of modules. Activate by running
  `cdf-tk features set MODULES_CMD --enable`, and deactivate by running `cdf-tk features set MODULES_CMD --disable`.
  Run `cdf-tk modules init/upgrade` to interactively initialize or upgrade modules.
  
## Fixed

- When running `cdf-tk build`, you would get a `DuplicatedItemWarning` on RAW Databases that are used with multiple
  tables. This is now fixed.

### Added

- Preview feature `MODULES_CMD` to allow interactive init and automatic upgrade of modules. Activate by running
  `cdf-tk features set MODULES_CMD --enable`, and deactivate by running `cdf-tk features set MODULES_CMD --disable`.
  Run `cdf-tk modules init/upgrade` to interactively initialize or upgrade modules.

## [0.2.0] - 2024-06-10

### Fixed

- When running `cdf-tk clean` or `cdf-tk deploy --drop --drop-data` there was an edge case that triggered the bug
  `ValueError: No capabilities given`. This is now fixed.
- When deploying `containers` resources with an index, the `cdf-tk deploy` would consider the resource as changed
  even though it was not. This is now fixed.
- When parsing yaml without `libyaml`, `cognite-toolkit` would raise an
  `AttributeError: module 'yaml' has no attribute 'CSafeLoader'`. This is now fixed by falling back to the
  python `yaml` parser if `libyaml` (c-based) is not available.

## [0.2.0b4] - 2024-06-06

### Added

- Support for resource type `TransformationNotification` in the `transformations` folder.

### Changed

- [BREAKING] In `functions`, the function config file must be in the root function directory. This means
  that, for example, `my_module/function/some_folder/function.yaml` will no longer be included by
  the `cdf-tk build` command. Instead, it must be in `my_module/function/function.yaml`. The motivation
  is to allow arbitrary YAML files as part of the function code.
- The toolkit now only gives a `TemplateVariableWarning` (`Variable my_variable has value <change_me> ...`) if
  the variable is used by `selected` in the `config.[env].yaml`. This is to avoid unnecessary warnings.
- The `FeaturePrevieWarnings` are no longer printed when running `cdf-tk deploy` or `cdf-tk clean`. These warnings
  are from the `cognite-sdk` and can be confusing to the user.

### Fixed

- When running `cdf-tk init --upgrade` from version `0.1.4` the user would get a `ToolkitMigrationError`.
  This is now fixed.

## [0.2.0b3] - 2024-06-04

### Added

- Support for resource type `Label` in the  `labels` folder.

### Fixed

- The toolkit now ensures `Transformations` and `Functions` are deployed before `Workflows`
- The toolkit now ensures `TimeSeries` and `Groups` are deployed before `DatapointSubscriptions`.

## [0.2.0b2] - 2024-06-03

### Fixed

- Running the build command, `cdf-tk build`, with `Group` resources scoped will read to incorrect
  warning such as `WARNING [HIGH]: Space 'spaceIds' is missing and is required by:` and
  `WARNING [HIGH]: DataSet 'ids' is missing and is required by:`. This is now fixed.
- Running the build command, `cdf-tk build`, with a `View` resource with a `hasData` filter would print a
  `UnusedParameterWarning: Parameter 'externalId' is not used in section ('filter', 'hasData', 0, 'externalId').`.
  This is incorrect and is now fixed to not print this warning.
- If you had a `container` with a direct relation property with a required constraint, the `cdf-tk build` command
  would incorrectly yield a warning that the `Parameter 'type' is not used ...`. This is now fixed.

## [0.2.0b1] - 2024-05-20

### Added

- Support for loading `nodes` with `APICall` arguments. The typical use case is when `node types` are part of a
  data model, and the default `APICall` arguments works well.

### Fixed

- Error message displayed to console on failed `cdf-tk deploy` command could be modified. This is now fixed.
- Using display name instead of folder name on a failed `cdf-tk deploy` or `cdf-tk clean` command. For example,
  if `datapoints subscription` was failing the error message would be `Failure to load/deploy timeseries as expected`,
  now it is `Failure to load/deploy timeseries.subscription as expected`.
- Unique display names for all resource types.
- Fixed bug when deploying extraction pipeline config, when none existed from before:
  `There is no config stored for the extraction pipeline`.

### Changed

- In `config.[env].yaml`, in the `environment` section, `selected_modules_and_packages` is renamed to `selected`.
  The old names will still work, but will trigger a deprecation warning.

## [0.2.0a5] - 2024-05-28

### Added

- If a resource is referring to another resource, the `cdf-tk build` will now give a warning if the referred resource
  is not found in the same build. For example, if you have a data model and is missing the space, the build command
  will give a warning that the space required by the data model is missing.
- The `cdf-tk build` command will now give warnings on duplicated resource. For example, if you have two files with
  the same externalId in the same module, the build command will give a warning that the externalId is duplicated,
  and that only the first file is used.
- Support for `securityCategories` in the `auth` folder.
- Added support for resource type `DatapointSubscription` in the `timeseries` folder.

### Fixed

- In a `function` config, if you did not set `fileId` you would get an error when running `cdf-tk deploy`,
  `Missing required field: 'fileId'.`. The `fileId` is generated automatically when the function is created,
  so it is not necessary to set it in the config file. This is now fixed.
- If you do `cdf-tk init --upgrade`, on a pre `0.2.0a3` version, you are met with
  `ERROR (ToolkitModuleVersionError): Failed to load previous version, ...`. This is now fixed.
- The parameter `container.properties.<property>.type.list` was required to be set, even thought it is optional
  in the CDF API. This is now fixed.
- The `ExtractionPipelineConfig` create, update and delete report numbers were incorrect. This is now fixed.

### Improved

- Gives a more informative error message when the authentication segment of a transformation resource file is
  missing a required field.
- Transformation queries can be inline, i.e. set in either the Transformation `query` property in the yaml or
  as a separate file. If set in both, an error is raised because it is ambiguous which query to use.
- In the `cdf-tk pull` command, if an error occurs, the temporary directory was not removed. This is now fixed.
- Improved error message when running `cdf-tk deploy/clean` before running `cdf-tk build`.

### Changed

- [BREAKING] In function YAML config `externalDataSetId` is renamed to `dataSetExternalId` to be consistent with
  the naming convention used in the rest of the toolkit.

## [0.2.0a4] - 2024-04-29

### Removed

- [BREAKING] `cognite-tookit` no longer supports `common_function_code`. The code used by functions must be in each
  function directory. The reason for this is that `cognite-toolkit` is a tool for governance and deployment of
  modules, it is not for development of functions. The `common_function_code` was a feature to support easier
  development of functions. It is expected that functions are developed in a separate environment and then
  moved to the `cognite_modules` folder for deployment and promotion between environments.

### Changed

- In `config.[env].yaml`, in the `environment` section, `name` is no longer required. Instead, the `[env]` part
  of the `config.[env].yaml` file is used as the `name` of the environment. This is to avoid redundancy.

### Improved

- When running `cdf-tk clean --dry-run` the output would show local resources regardless of whether they existed
  in CDF or not. This is now fixed and only resources that exist in CDF are shown in the output.
- Better error message (no exception raised) if the config file has `selected_modules_and_packages`, but with no list items.
- If yaml files are invalid, a link to the API docs for the resource is shown in the error message.

### Fixed

- When deploying a `FunctionSchedule` that requires an update, the `cdf-tk` would fail with error
  `Failed to update functions.schedules. Error 'FunctionSchedulesAPI' object has no attribute 'update'.`.
  This is now fixed.
- When calling `cdf-tk init --upgrade`, the user is met with a `Failed to load previous version, ...`.
  This is now fixed.
- When running `cdf-tk auth verify --interactive` and the user want to create a new group with the necessary
  capabilities, the `cdf-tk` would successfully create a group, but then raise
  an Error: `cognite.client.exceptions.CogniteAPIError: Insufficient access rights.` when trying to validate.
  This is now fixed.

## [0.2.0a3] - 2024-04-23

### Added

- Support for the Workflow and WorkflowVersion resource type
- Support for specifying `selected_modules_and_packages` as paths and parent paths. For example, you can
  now write `cognite_modules/core/cdf_apm_base` instead of just `cdf_apm_base`. This is to support
  modules that have the same name but are in different parent directories. In addition, this also better reflects
  the structure of the `cognite_modules` and `custom_modules` folder better.

### Fixed

- Functions that are deployed with schedules no longer uses a short-lived session (before: failed after ~an hour).

### Changed

- [BREAKING] The `cdf-tk build` will now clean the build directory by default before building the modules to avoid
  unwanted side effects from previous builds. To stop this behavior, use the `--no-clean` flag.
- [BREAKING] The `_system.yaml` is now required to be on the root level when running any `cdf-tk` command. This
  means that the `_system.yaml` file must be in the same directory as the `config.[env].yaml` files. This is to
  support running `cdf-tk` without the `cognite_modules` folder.
- [BREAKING] In the `config.[env].yaml` files, the `modules` section is now renamed to `variables`. This is to
  better reflect the content of this section, which is variables that can be used in the resource files.
- In addition to `cognite_modules` and `custom_modules`, the `cognite-toolkit` now also support `modules` as the
  top-level folder for modules. This together with the two changes above, is to have a better support for running
  the `cognite-toolkit` as a standalone CLI without the `cognite_modules`.
- The `.gitignore` file you get by running `cdf-tk init` now ignores the `/build` by default.
- The dependency `cognite-sdk` must now be `>=7.37.0` to use the `cdf-tk`.

## [0.2.0a2] - 2024-04-03

### Added

- Variables can now have extra spaces between curly braces and the variable name. For example, `{{  my_variable }}` is now
  a valid variable. Before this change, you would have to write `{{my_variable}}`.
- If an environment variable is not found in a resource file, for example, `${CDF_CLUSTER}`, when
  running `cdf-tk deploy` the user will now get a warning message that the variable is missing. Before this change,
  this would pass silently and potentially cause an error when trying to deploy to CDF that was hard to debug.

### Fixed

- When running `cdf-tk` with a Token for initialization, the `cdf-tk` would raise an `IndexError`. This is now fixed.
- Container resources that did not have set the optional property `usedFor` would always be classified as changed,
  when, for example, running `cdf-tk deploy --dry-run`. This is now fixed.

### Changed

- If two modules have the same name, the `cdf-tk build` command will now stop and raise an error. Before this change,
  the `cdf-tk build` command would continue and overwrite the first module with the second module.

## [0.2.0a1] - 2024-03-20

### Added

- Support for interactive login. The user can now set `LOGIN_FLOW=interactive` in the `.env` file
  to use interactive login.

### Changed

- The verification of access by the tool is now scoped to the resources that are being deployed instead of
  the entire project. This means that if the user only has access to a subset of the resources in the project,
  the tool will still be able to deploy those resources.

## [0.1.2] - 2024-03-18

### Fixed

- Running the command `cdf-tk auth verify --interactive` without a `.env` would raise a
  `AttributeError: 'CDFToolConfig' object has no attribute '_client'` error. This is now fixed and instead the user
  gets a guided experience to set up the `.env` file.

### Changed

- `cognite-toolkit` have moved the upper bound on the `cognite-sdk` dependency from `7.27` to `8.0`.
- Creating/Removing `spaces` no longer requires `DataModelingInstances` capability.

## [0.1.1] - 2024-03-01

### Fixed

- When running `cdf-tk clean` or `cdf-tk deploy --drop-data` for a data model with more than 10 containers,
  the command would raise an APIError. This is now fixed.
- A few minor potential `AttributeError` and `KeyError` bugs have been fixed.

## [0.1.0] - 2024-02-29

### Added

- Command `cdf-tk dump datamodel` for dumping data models from CDF into a local folder. The use case for this is to
  dump an existing data model from CDF and use it as a basis for building a new custom module with that data model.
- A Python package API for the cdf-tk. This allows for programmatic access to the cdf-tk functionality. This
  is limited to the `build` and `deploy` functionality. You can start by `from cognite_toolkit import CogniteToolkit`.

### Fixed

- In the function deployment, the hashing function used of the directory was independent of the location of the files
  within the function directory. This caused moving files not to trigger a redeployment of the function. This is now
  fixed.

### Changed

- Removed unused dependencies `mypy`, `pyarrow` and `chardet` from `cognite-toolkit` package.
- Lowered the required version of `pandas` to `1.5.3` in the `cognite-toolkit` package.

## [0.1.0b9] - 2024-02-20

### Added

- Introduced `cdf-tk pull transformation` and `cdf-tk pull node` commands to pull transformation or nodes
  from CDF to the local module.
- Support for using a template for file names `name: prefix_$FILENAME_suffix` in the `files` resource. The files will
  be processed and renamed as part of the build step.

### Fixed

- Fixed a bug that caused `Group` upsert to leave duplicate Groups
- Fixed issue with `run function --local` that did not pick up functions in modules without config variables.
- Fixed error when running `run function --local` on a function without all optional parameters for handle() being set.
- Bug when `cdf-tk deploy` of `ExtractionPipelineConfig` with multiple `config` objects in the same file.
  Then only the first `config` object was deployed. This is now fixed.

### Changed

- `cdf-tk` now uses --external-id consistently instead of --external_id.
- Removed upper limit on Python version requirement, such that, for example, `Python 3.12` is allowed. Note
  that when working with `functions` it is recommended to use `Python 3.9-3.11` as `Python 3.12` is not
  supported yet.
- `cdf-tk deploy`/`cdf-tk clean` now deploys all config files in one go, instead of one by one. This means batching
  is no longer done based on the number of resource files, but instead based on the limit of the CDF API.
- Files in module directories that do not live in a recognised resource directory will be skipped when building. If
  verbose is enabled, a warning will be printed for each skipped file.
- Only .yaml files in functions resource folders and the defined function sub-directories will be processed as part of
  building.

## [0.1.0b8] - 2024-02-14

### Added

- `Group` resource type supports list of groups in the same file

### Fixed

- `View` which implements other views would always be classified as changed, ven though no change
  has been done to the `view`, in the `cdf-tk deploy` command. This is now fixed.
- `DataModels` which are equal would be wrongly classified as changed if the view order was different.
  This is now fixed.
- In the `cdf-tk build`, modules with a nested folder structure under the resource folder were not built correctly.
  For example, if you had `my_module/data_models/container/my_container.container.view`, it would be put inside
  a `build/container/my_container.container.yaml` instead of `build/data_models/my_container.container.yaml`,
  and thus fail in the `cdf-tk deploy/clean` step. This is now fixed.
- When running `cdf-tk deploy` the prefixed number on resource file was not used to sort the deployment order.
  This is now fixed.
- Fixed a bug that caused Extraction Pipeline Config update to fail

## [0.1.0b7] - 2024-02-07

### Added

**NOTE: The function changelog was by accident included in beta6 and has been moved to the correct version.**

- Added support for loading functions and function schedules. Example of a function can be found in `cognite_modules/example/cdf_functions_dummy`.
- Added support for common function code as defined by `common_function_code` parameter in the environment config file.
- Added support for new command, `run function` that runs a function with a one-shot session created using currently
  configured credentials for cdf-tk.
- Added support for running a Cognite function locally using the `run function --local` command. This command will run the
  function locally in a virtual environment simulating CDF hosted run-time environment and print the result to the console.

### Changed

- **BREAKING:** The cdf-toolkit now requires one `config.yaml` per environment, for example, `config.dev.yaml` and `config.prod.yaml`.
- **BREAKING:** The file `environments.yaml` has been merged into `config.[env].yaml`.
  This means that the `environments.yaml` file is no longer used and the `config.[env].yaml`
  file now contains all the information needed to deploy to that environment.
- The module `cognite_modules` is no longer considered to be a black box governed by the toolkit, but should instead
  be controlled by the user. There are two main changes to the `cognite_modules` folder:
  - All `default.config.yaml` are removed from `cognite_modules` and only used when running `cdf-tk init`to generate
    `config.[env].yaml` files.
  - The file `default.packages.yaml` has been renamed `_system.yaml` and extended to include the `cdf-tk` version.
    This should not be changed by the user and is used to store package information for the toolkit itself and
    version.
- Running the `cdf-tk init --upgrade` now gives the user instructions on how to update the breaking changes
  since their last upgrade.
- If the user has changed any files in `cognite_modules`, the command `cdf-tk init --upgrade` will no longer
  overwrite the content of the `cognite_modules` folder. Instead, the user will be given instructions on how to
  update the `cognite_modules` files in the folder manually.

### Fixed

- In the generation of the `config.[env].yaml` multiline comments were lost. This is now fixed.

## [0.1.0b6] - 2024-01-25

### Added

- In `raw` resources, a RAW database or tables can be specified without data. Example, of a single database

 ```yaml
dbName: RawDatabase
```

or a database with table, no need to also specify a `.csv` or `.parquet` file for the table as was necessary before.

```yaml
dbName: myRawRawDatabase
tableName: myRawTable
```

### Changed

- Update is implemented for all resources. This means that if a resource already exists and is exactly the same as
  the one to be deployed, it will be updated instead of deleted and recreated.
- The `cdf-tk deploy` `--drop-data` is now independent of the `--drop` flag. This means that you can now drop data
  without dropping the resource itself. The reverse is not true, if you specify `--drop` without `--drop-data`, only
  resources that can be deleted without dropping data will be deleted.
- The output of the `cdf-tk deploy` command has been improved. Instead of created, deleted, and skipped resources
  being printed in a table at the end of the command, the resources are now printed as they are created, deleted, changed,
  and unchanged. In addition, an extra table is printed below with the datapoints that have been uploaded and dropped.
- The output of the `cdf-tk clean` command has also been changed in the same way as the `cdf-tk deploy` command.
- The `files` resource has been split into two resources, `FileMetadata` and `Files` to separate the metadata from
  the data (the file).
- To ensure comparison of resources and be able to determine whether they need to be updated, any resource
  defined in a YAML file will be augmented with default values (as defined by the CDF API) if they are missing before
  they are deployed.

### Fixed

- Bug in `auth` resource, this caused  groups with `all` and `resource` scoped capabilities to be written in two steps
  first with only `all` scoped capabilities and then all capabilities. This is now fixed by deploying groups in
  a single step.

## [0.1.0b5] - 2024-01-11

### Added

- Support for custom environment variables injected into build files when calling the command `cdf-tk deploy`.
- All resources that are unchanged are now skipped when running `cdf-tk deploy`.
- Support for loading `Group` Capabilities with scope `idScope` of type string. This means you can now set the
  `idScope` to the external id of a `dataSet` and it will be automatically replaced by the dataset id
  `cdf-tk deploy`.

### Fixed

- Fixed bug when calling any command loading a `.env` file and the path is not relative to the current working
  directory. This is now fixed.
- Calling `cdf-tk init --upgrade` overwrote all variables and comments set in the `config.yaml` file. This is now
  fixed.

### Improved

- Improved error message when missing a variable in `config.yaml` and a variable with the same name is defined
  for another module.

## [0.1.0b4] - 2024-01-08

### Added

- Added `--env-path` option to specify custom locations of `.env` file

### Fixed

- Fixed bug in command `cdf-tk build` that can occur when running on `Python>=3.10` which caused an error with text
  `TypeError: issubclass() arg 1 must be a class`. This is now fixed.

## [0.1.0b3] - 2024-01-02

### Fixed

- Fixed bug in `cdf-tk deploy` where auth groups with a mix of all and resource scoped capabilities skipped
  the all scoped capabilities. This is now fixed.

## [0.1.0b2] - 2023-12-17

### Fixed

- Handle duplicate `TransformationSchedules` when loading `Transformation` resources.
- Print table at the end of `cdf-tk deploy` failed with `AttributeError`, if any of resources were empty.
  This is now fixed.
- The `cdf-tk build` command no longer gives a warning about missing `sql` file for
  `TransformationSchedule`s.

## [0.1.0b1] - 2023-12-15

### Added

- Warnings if a configuration file is using `snake_case` when then resource type is expecting `camelCase`.
- Added support for validation of `space` for data models.
- Check for whether template variables `<change_me>` are present in the config files.
- Check for whether data set id is present in the config files.
- Print table at the end of `cdf-tk deploy` with the resources that were created, deleted, and skipped.
- Support for Extraction Pipelines and Extraction Pipeline configuration for remotely configured Extractors
- Separate loader for Transformation Schedule resources.

### Removed

- In the `deploy` command `drop_data` option has been removed. To drop data, use the `clean` command instead.

### Changed

- Require all spaces to be explicitly defined as separate .space.yaml file.
- The `data_set_id` for `Transformations` must now be set explicitly in the yaml config file for the `Transformation`
  under the `data_set_id` key. Note that you also need to explicitly define the `data_set` in its own yaml config file.
- All config files have been merged to a single config file, `config.yaml`. Upon calling `cdf-tk init` the `config.yaml`
  is created in the root folder of the project based on the `default.config.yaml` file of each module.
- DataSetID is no longer set implicitly when running the `cdf-tk deploy` command. Instead, the `data_set_id` must be
  set explicitly in the yaml config file.

### Fixed

- When running `cdf-tk deploy` with `--dry-run` a `ValueError` was raised if not all datasets were pre-existing.
  This is now fixed by skipping dataset validation when running with `--dry-run`.
- When having a `auth` group with mixed capabilities of all scoped and resource scoped, the all scoped capabilities
  were not removed when running `cdf-tk deploy`. This is now fixed.
- Loading `Transformation` did not support setting `dataSetExternalId` in the yaml config file. This is now fixed.

## [0.1.0a3] - 2023-12-01

### Changed

- Refactored load functionality. Loading raw tables and files now requires a `yaml` file with metadata.
- Fix container comparison to detect identical containers when loading data models (without --drop flag).
- Clean up error on resource does not exist when deleting (on `deploy --drop` or using clean command).

### Added

- Support for loading `data_sets`.
- Support for loading auth without --drop, i.e. `deploy --include=auth` and only changed groups are deployed.
- `cdf-tk --verbose build` now prints the resolution of modules and packages.
- Added `cdf-tk --version` to print the version of the tool and the templates.
- Support for `upsert` for `data_sets`.
- The cmd `cdf-tk deploy` creates the `data_set` before all other resources.
- Data sets are no longer implicitly created when referenced by another resource, instead an error is raised.
- Require all spaces to be explicitly defined as separate .space.yaml file.
- Add protection on group deletion and skip any groups that the current service principal belongs to.
- Support for multiple file resources per yaml config file for files resources.
- Support templated loading of * files in a folder when a single yaml has `externalId: something_$FILENAME`.
- You can now name the transformation .sql either with the externalId (as defined in the
  corresponding yaml file) or with the name of the file of the corresponding yaml file.
  I.e. if a transformation is defined in my_transformation.yaml with externalId:
  `tr_something`, the SQL file should be named either `tr_something.sql` or `my_transformation.sql`.
- Missing .sql files for transformations will now raise an error in the build step.
- The build step will now raise a number of warnings for missing externalIds in the yaml files,
  as well as if the naming conventions are not followed.
- System section in `environments.yaml` to track local state of `cdf-toolkit`.
- Introduced a `build_environment.yaml` in the `/build` folder to track how the build was run.

### Fixed

- `cdf-tk clean` not supporting `--include` properly.
- `cdf-tk clean` not working properly for data models with data.
- Fix group deletion on use of clean command to actually delete groups.

## [0.1.0a2] - 2023-11-22

### Fixed

- The `experimental` module was not included when running command `cdf-tk init`. This is now fixed.

## [0.1.0a1] - 2023-11-21

Initial release
