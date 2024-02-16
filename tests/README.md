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

* Any unexpected exceptions thrown by running the commands.
* Any unexpected changes to what is written to CDF.

**Note** that when you add a new module or do a change to an existing module that expects to change what is
written to CDF, these tests will fail.

* If you have added a new module, the first time you run the tests, the tests will fail as there is no previous
  output to compare to. This will automatically create a new snapshot file `.yaml` and the next time you run
  the tests, the tests will pass if the output produced is the same as the previous output.
* If you have changed an existing module, the tests will fail, and you need to manually verify that the changes
  are expected and then update the snapshot file. You do this by running the tests with the `pytest --force-regen`

## <code>ApprovalCogniteClient</code>

To intercept CDF calls, we have created an `ApprovalCogniteClient` that is used in the snapshot tests. To understand
this client, you need to understand mocking. The first Section below goes through the basics of mocking in the CDF client
context. The second section goes through how the `ApprovalCogniteClient` is built, and the last how to extend it.

**Note** that the `ApprovalCogniteClient` is not only used in the snapshot tests, but also in other tests where we want
to simulate CDF.

### Mocking CDF Client

From the `cognite-sdk` there is a built-in mock client available. This can be used as shown below:

```python
from cognite.client.testing import CogniteClientMock
from cognite.client import CogniteClient
from cognite.client.data_classes import Asset, AssetList

def my_function() -> list[str]:
    client = CogniteClient()
    assets = client.assets.list()
    return [asset.name for asset in assets]


def tests_my_function():
    expected_asset_names = ["MyAsset"]
    
    with CogniteClientMock() as client_mock:
        client_mock.assets.list.return_value = AssetList([Asset(id=1, name="MyAsset")])
        actual_asset_names = my_function()
    
    assert actual_asset_names == expected_asset_names
```

In the example above, we have a function `my_function` that uses the `CogniteClient` to get the names of all assets.
In the test `tests_my_function` we use the `CogniteClientMock` to mock the `CogniteClient` and set the return value
of the `assets.list` method to a list of assets with the name `MyAsset`. We then call `my_function` and verify that
the return value is as expected.

### ApprovalClient

A challenge with the example above is that there are typically 3–5 methods that need to be mocked for each resource
type. As of, 16. February 2024, the `cognite-toolkit` supports 18 different resource types, which means that the
number of methods that need to be mocked is ~72. In addition, the mocking of these calls we want to be able
to support the following:

* The ability to check which calls have been made. For example, when we run `cdf-tk deploy --dry-run` no `create`
  or `delete` calls should be made to CDF.
* We need to be able to get all the resources that have been created, updated, or deleted. This is used in the
  snapshot tests to verify that the output is as expected.
* We need to be able to get the resource to return a resource we have in a test, so we can check that the rest
  of the function works as expected.
* By default, all `list` and `retrieve` calls should return empty lists and `None`, respectively. Instead of
  a mock object which is the default behavior of the `CogniteClientMock`.

### Extending the ApprovalClient
