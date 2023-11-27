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

## [0.1.1] - 2023-11-23

### Changed
- Changed format of infield external_ids to be more readable, moving _dataset (ds) and _space to the beginning of the external_id.
- `examples/cdf_apm_simple/raw` and `examples/example_dump_asst_hierarchy/raw` now explicitly
  defines database and table name in `.yaml` files for each table.
- Added `data_set` to `examples/example_dump_asst_hierarchy/`, which was implicitly defined in
  before.

### Fixed

- cdf_infield_common module and the auth applications-configuration.yaml did not load group source id
   correctly due to source_id being used instead of sourceId. This is now fixed.

## [0.1.0] - 2023-11-21

Initial release

