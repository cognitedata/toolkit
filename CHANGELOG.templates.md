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
- 
## TBD - 2023-12-TBD
### Added
- Explicitly define model `space` in `experimental/cdf_asset_source_model/` and `experimental/example_pump_model/`.
- The module `my_example_module` has been added to the `custom_modules` folder.
### Changed
- All cognite templates have been moved into `cognite_templates` folder, while `local_templates` is renamed to `custom_templates`.
- Move cdf_apm_base into separate folder.
- The file `local.yaml` has been renamed `environments.yaml` to better reflect its purpose.
## [0.2.0] - 2023-12-01

### Changed

- **BREAKING** All externalIds and names have been changed to follow the naming conventions for resources
  in `examples/cdf_oid_example_data`, `examples/cdf_apm_simple_data_model`, `modules/cdf_apm_base`,
  `modules/cdf_infield_common`, and `modules/cdf_infield_location`.

## [0.1.2] - 2023-11-29

### Changed

- Remove unused template_version variable from groups and use of group metadata.
- Split up cdf_oid_example_data into data sets and RAW databases per source system.

### Fixed

- Add space yaml files for existing data models when explicit space definition was introduced.
- Fix use of integer value in version for data models.
- Fix wrong reference to `apm_simple` in `examples/cdf_apm_simple_data_model` and `modules/cdf_infield_location`.
- Exemplify use of a single config yaml file for multiple file resources in `examples/cdf_oid_example_data/files/files.yaml`.

## [0.1.1] - 2023-11-23

### Changed

- Changed format of infield external_ids to be more readable, moving `_dataset` (ds) and `_space` to the beginning of the external_id.
- `examples/cdf_apm_simple/raw` and `examples/example_dump_asst_hierarchy/raw` now explicitly
  defines database and table name in `.yaml` files for each table.
- Added `data_set` to `examples/example_dump_asst_hierarchy/`, which was implicitly defined in
  before.

### Fixed

- cdf_infield_common module and the auth applications-configuration.yaml did not load group source id
   correctly due to source_id being used instead of sourceId. This is now fixed.

## [0.1.0] - 2023-11-21

Initial release
