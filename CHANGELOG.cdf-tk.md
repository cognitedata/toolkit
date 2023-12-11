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

## [TBD] - 2023-12-TBD
### Added
- Warnings if a configuration file is using `snake_case` when then resource type is expecting `camelCase`.
- Added support for validation of `space` for data models.
- Print table at the end of `cdf-tk deploy` with the resources that were created, deleted, and skipped.
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

### Fixed

- `cdf-tk clean` not supporting `--include` properly.
- `cdf-tk clean` not working properly for data models with data.
- Fix group deletion on use of clean command to actually delete groups.

## [0.1.0a2] - 2023-11-22

### Fixed

- The `experimental` module was not included when running command `cdf-tk init`. This is now fixed.

## [0.1.0a1] - 2023-11-21

Initial release
