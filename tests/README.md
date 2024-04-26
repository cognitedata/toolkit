# Testing

Tests are organized into two main categories, integration and unit tests.
The integration tests are dependent on the CDF, while the unit tests are not:

```bash
📦tests
 ┣ 📂tests_integration - Tests depending on CDF.
 ┣ 📂tests_unit - Tests without any external dependencies.
 ┣ 📜constants.py - Constants used in the tests.
 ┗ 📜README.md - This file
```

## Integration Testing

To support the integration testing you need to have a `.env` file in the root of the project following the
[`.env.templ`](../cognite_toolkit/.env.templ) file structure.

## Unit Testing

### Snapshot Testing (Regression Testing)

In `cdf-tk`, we use snapshot testing to check that a sequence of CLI commands produces consistent output. This type
of testing is also called [Regression Testing](https://en.wikipedia.org/wiki/Regression_testing). Note the
idea of snapshot testing can be used in integration tests as well, but in `cdf-tk` we only use it
in the unit tests.

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

### <code>ApprovalCogniteClient</code>

To intercept CDF calls, we have created an `ApprovalCogniteClient` that is used in the snapshot tests. To understand
this client, you need to understand mocking. The first Section below goes through the basics of mocking in the CDF client
context. The second section goes through how the `ApprovalCogniteClient` is built, and the last how to extend it.

**Note** that the `ApprovalCogniteClient` is not only used in the snapshot tests, but also in other tests where we want
to simulate CDF.

#### Mocking CDF Client

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

#### ApprovalClient

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

The approval client is put into a package called `approval_client` and is organized as follows:

```bash
```bash
📦approval_client
 ┣ 📜client.py - Module withe the ApprovalCogniteClient class
 ┣ 📜config.py - Configuration of which API methods are mocked by which mock functions.
 ┗ 📜data_classes.py - Helper classes for the ApprovalCogniteClient
```

Each of the public methods in the `ApprovalCogniteClient` should be documented with the most up-to-date information.

The most important methods/properties in the `ApprovalCogniteClient` are:

* Property `client` - Returns a mocked `CogniteClient`
* Method `append` - Allows you to append a resource to the client. This is used to simulate
  the resources that are returned from CDF.
* Method `dump` - Dumps all resources that have been created, updated, or deleted to a dictionary.

#### Extending the ApprovalClient

To extend the `ApprovalCogniteClient`, you either reuse existing mock functions or you have to create new ones.

The most important private methods in the `ApprovalCogniteClient` are:

* `_create_delete_method` - Creates a mock function for the `delete` methods in the API call
* `_create_create_method` - Creates a mock function for the `create` methods in the API call
* `_create_retrieve_method` - Creates a mock function for the `retrieve` methods in the API call

Inside each of these methods, you will find multiple functions that are used to create the mock functions. Which
function is used to mock which methods are controlled by the constant `API_RESOURCES` in `config.py`. This constant
is a list of APIClasses with the resource classes and which methods are mocked by which mock functions. If you
can reuse an existing mock function, you can add the resource class to the list and set the mock function to the
existing mock function. If you need to create a new mock function, you need to create a new function and add it
inside the relevant method in the `ApprovalCogniteClient`.

The reason why we have multiple different mock functions for the same type of methods is that the `cognite-sdk`
does not have a uniform way of handling the different resource types. For example, the `create` method
for most resource is create and takes a single or a list of the resource type `client.time_series.create` is
an example of this. However, when creating a `datapoints` the method is `client.time_series.data.insert` and takes a
`pandas.DataFrame` as input, same for `sequences`. Another exception if `client.files.upload_bytes` which is used
to upload files to CDF.
