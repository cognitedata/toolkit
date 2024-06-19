# Module Upgrade

This directory contains the script `run_check.py` that is used to check for breaking changes in the package,
and that the `cdf-tk module upgrade` command works as expected.

## Motivation

This could have been part of the test suite, but it is not for two reasons:

* It needs to have project_inits for each version of the package, each at the size of `~25MB` which is too large to
  include in the test suite.
* Running the check is time consuming, and only needs to be run before a new release.

## Workflow

1. The constant `cognite_toolkit/_cdf_tk/constants.py:SUPPORT_MODULE_UPGRADE_FROM_VERSION` controls the
   earliest version that the `cdf-tk module upgrade` command should support.
2. Run `python module_upgrade/run_check.py` to check that the `cdf-tk module upgrade` command works as expected.
   If any exceptions are raised, you need to update the `_changes.py` file in the `modules` commands, so that the
   `cdf-tk module upgrade` command works as expected.
