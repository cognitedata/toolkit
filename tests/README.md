# Testing

## Integration vs Unit Testing

Tests are organized into two main categories, integration and unit tests.
The integration tests are dependent on the CDF, while the unit tests are not:

```bash
📦tests
 ┣ 📂tests_integration - Tests depending on CDF.
 ┣ 📂tests_unit - Tests without any external dependencies.
 ┣ 📜constants.py - Constants used in the tests.
 ┗ 📜README.md - This file
```

## Snapshot Testing

In `cdf-tk` we use snapshot testing to check that a sequence of CLI commands produces consistent output. This type
of testing is also called [Regression Testing](https://en.wikipedia.org/wiki/Regression_testing).

We have three main types of snapshot tests in `cdf-tk`:

1. `tests/tests_unit/test_approval_modules.py:test_deploy_module_approval` Doing the deployment for each module:
   1. `cdf-tk init`
   2. `cdf-tk build`
   3. `cdf-tk deploy`
2. `tests/tests_unit/test_approval_modules.py:test_deploy_dry_run_module_approval` Doing the deployment for each
   module with `--dry-run`
   1. `cdf-tk init`
   2. `cdf-tk build`
   3. `cdf-tk deploy --dry-run`
3. `tests/tests_unit/test_approval_modules.py:test_clean_module_approval` Doing the clean for each module:
   1. `cdf-tk init`
   2. `cdf-tk build`
   3. `cdf-tk clean`

These tests run the above commands, intercepts the calls to CDF, dumps the calls to a file, and compares the
output to the previous output of running the same commands for the same modules. See,
for example, [cdf_apm_simple](tests_unit/test_approval_modules_snapshots/cdf_apm_simple.yaml) to see an
output of a snapshot test.

The typical errors these tests' catch are:

* Any unexpected exceptions thrown by the CLI commands.
* Any unexpected changes to what is written to CDF.

**Note** that when you add a new module or do a change to an existing module that expects to change what is
written to CDF, these tests will fail.

* If you have added a new module, the first time you run the tests, the tests will fail as there is no previous
  output to compare to. This will automatically create a new snapshot file `.yaml` and the next time you run
  the tests, the tests will pass if the output produced is the same as the previous output.
* If you have changed an existing module, the tests will fail and you need to manually verify that the changes
  are expected and then update the snapshot file. You do this by running the tests with the `pytest --force-regen`

## Approval Client
