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

## Task Guides

This section contains guides on how to do different tasks related to the ApprovalClient

### Debugging Function Calls to CDF

Sometimes you want to debug the function calls to CDF. This can be to check what is actually written, or try to make
sense of an error message. It is not easy to use a debugger to step through the call to CDF, as you will be taken
through a maze of code that is related to the mocked client. In the example below, if you set a breakpoint at
`print("break here")` and run the code, if the `client` is mocked, you will have trouble stepping into the
`insert_dataframe` call.

```python
import pandas as pd

data = pd.DataFrame(
    {
        "timestamp": [1, 2, 3],
        "value": [1, 2, 3],
    }
)
print("break here")
client.raw.rows.insert_dataframe(
    db_name="my_db_name", table_name="my_table", dataframe = data, ensure_parent = False
)
```

Instead, you can check in the `tests_unit/approval_client/config.py` to figure out which method is used to mock
the `.insert_dataframe` call, looking through the file you will find this section:

```python
    APIResource(
        api_name="raw.rows",
        resource_cls=Row,
        _write_cls=RowWrite,
        list_cls=RowList,
        _write_list_cls=RowWriteList,
        methods={
            "create": [Method(api_class_method="insert_dataframe", mock_class_method="insert_dataframe")],
            "delete": [Method(api_class_method="delete", mock_class_method="delete_raw")],
            "retrieve": [
                Method(api_class_method="list", mock_class_method="return_values"),
                Method(api_class_method="retrieve", mock_class_method="return_values"),
            ],
        },
    ),
```

Some extra explanation of the code above:

* The `_write_cls` and `_write_list_cls` are the private, as the `write_cls` and `write_list_cls` are properties
  which uses the `resoruce_cls` and `list_cls` as fallbacks if the `_write_cls` and `_write_list_cls` are not set.
* In the methods dictionary, the key is the Loader classification. This is used to classify the type of method
  you are mocking. This is, for example, used to check `cdf-tk deploy --dry-run` do not make any `create` or `delete`
  calls. Note as of writing this `update` is not used in any tests, and have thus not been implemented.
* In the methods dictionary, the value is a linking between the method in the `cognite-sdk` and the mock function
  that is used to mock the method. The reason for this is to easily reuse mock methods for different `cognite-sdk`
  methods (very many of the `cognite-sdk` methods can be mocked in exactly the same way). You can see what mock methods
  are available by checking the functions inside each of the `_create_create_method`, `_create_delete_method`,
  and `_create_retrieve_method` methods in the `ApprovalCogniteClient`.

We see that the `create` method in the cognite-sdk for RAW rows is mocked by the `insert_dataframe` method in the mock
class. We can then go to the `tests_unit/approval_client/client.py` and find the `insert_dataframe` method inside the
private `_create_create_method` and set the breakpoint there. This will then stop the code execution when the
`insert_dataframe` method is called.

![image](https://github.com/cognitedata/toolkit/assets/60234212/aa8f72c9-0ecd-4166-bb41-f438fba25b4b)

### Simulate Existing Resource in CDF

You can simulate an existing resource in CDF by using the `append` method in the `ApprovalCogniteClient`. Below
is an example of a test that simulates an existing `Transformation` in CDF. Note that the `ApprovalCogniteClient`
does not do any logic when the `.retrieve` or `.list` methods are called, it just returns the resource that has
been appended to the client.

```python
def test_pull_transformation(
    monkeypatch: MonkeyPatch,
    cognite_client_approval: ApprovalCogniteClient,
    cdf_tool_config: CDFToolConfig,
    typer_context: typer.Context,
    init_project: Path,
) -> None:
    loader = TransformationLoader.create_loader(cdf_tool_config.toolkit_client)

    loaded = load_transformation()

    # Simulate a change in the transformation in CDF.
    loaded.name = "New transformation name"
    read_transformation = Transformation.load(loaded.dump())
    
    # Here we append the transformation to the ApprovalCogniteClient which 
    # simulates that the transformation exists in CDF.
    cognite_client_approval.append(Transformation, read_transformation)

    pull_transformation_cmd(
        typer_context,
        source_dir=str(init_project),
        external_id=read_transformation.external_id,
        env="dev",
        dry_run=False,
    )

    after_loaded = load_transformation()

    assert after_loaded.name == "New transformation name"
```

### Adding Support for a new Resource Type

When you add support for a new resource type, you need to first add the an entry to the
`approval_client/config.py` file which defines which methods are mocked by which mock functions.

If none of the existing mock functions can be used, you need to create a new mock function in the
`approval_client/client.py` file. The mock function should be added to the relevant method in the
`ApprovalCogniteClient` class.

You can check the following example PR when support was added for `Workflows` in the `ApprovalCogniteClient`:

[PR #453](https://github.com/cognitedata/toolkit/pull/453/files#diff-57825118cb6e6afb003f556137ba38af9f770c69f7223dfcaa2779413228e2f1R393)
