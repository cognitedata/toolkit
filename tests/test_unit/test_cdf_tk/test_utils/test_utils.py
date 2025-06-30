from __future__ import annotations

import shutil
import tempfile
from collections.abc import Iterable
from pathlib import Path
from typing import Any

import pytest
import yaml
from _pytest.mark import ParameterSet

from cognite_toolkit._cdf_tk.data_classes import BuildVariable, BuildVariables
from cognite_toolkit._cdf_tk.tk_warnings import (
    EnvironmentVariableMissingWarning,
    TemplateVariableWarning,
    catch_warnings,
)
from cognite_toolkit._cdf_tk.utils import (
    calculate_directory_hash,
    flatten_dict,
    iterate_modules,
    load_yaml_inject_variables,
    module_from_path,
    quote_int_value_by_key_in_yaml,
    stringify_value_by_key_in_yaml,
)
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
from cognite_toolkit._cdf_tk.utils.modules import module_directory_from_path
from cognite_toolkit._cdf_tk.validation import validate_modules_variables
from tests.data import DATA_FOLDER, PROJECT_FOR_TEST


class TestLoadYamlInjectVariables:
    def test_load_yaml_inject_variables(self, tmp_path: Path) -> None:
        my_file = tmp_path / "test.yaml"
        my_file.write_text(yaml_safe_dump({"test": "${TEST}"}))

        loaded = load_yaml_inject_variables(my_file, {"TEST": "my_injected_value"})

        assert loaded["test"] == "my_injected_value"

    def test_warning_when_missing_env_variable(self) -> None:
        path = Path("test.yaml")
        content = yaml_safe_dump({"test": "${TEST}"})
        expected_warning = EnvironmentVariableMissingWarning(path, frozenset({"TEST"}))

        with catch_warnings() as warning_list:
            load_yaml_inject_variables(content, {}, original_filepath=path)

        assert len(warning_list) == 1
        assert warning_list[0] == expected_warning


@pytest.mark.parametrize(
    "variable, expected_warnings",
    [
        pytest.param(
            BuildVariable("sourceId", "<change_me>", False, Path()),
            [TemplateVariableWarning(Path("config.yaml"), "<change_me>", "sourceId", "")],
            id="Single warning",
        ),
        pytest.param(
            BuildVariable("sourceId", "<change_me>", False, Path("a_module")),
            [TemplateVariableWarning(Path("config.yaml"), "<change_me>", "sourceId", "a_module")],
            id="Nested warning",
        ),
        pytest.param(
            BuildVariable("sourceId", "<change_me>", False, Path("a_super_module/a_module")),
            [TemplateVariableWarning(Path("config.yaml"), "<change_me>", "sourceId", "a_super_module.a_module")],
            id="Deep nested warning",
        ),
        pytest.param(BuildVariable("sourceId", "123", False, Path("a_module")), [], id="No warning"),
    ],
)
def test_validate_config_yaml(variable: BuildVariable, expected_warnings: list[TemplateVariableWarning]) -> None:
    warnings = validate_modules_variables(BuildVariables([variable]), Path("config.yaml"))

    assert sorted(warnings) == sorted(expected_warnings)


def test_calculate_hash_on_folder() -> None:
    folder = Path(DATA_FOLDER / "calc_hash_data")
    hash1 = calculate_directory_hash(folder)
    hash2 = calculate_directory_hash(folder)

    print(hash1)

    assert hash1 == "e60120ed03ebc1de314222a6a330dce08b7e2d77ec0929cd3c603cfdc08999ad", (
        f"The hash should not change as long as content in {folder} is not changed."
    )
    assert hash1 == hash2
    tempdir = Path(tempfile.mkdtemp())
    shutil.rmtree(tempdir)
    shutil.copytree(folder, tempdir)
    hash3 = calculate_directory_hash(tempdir)
    shutil.rmtree(tempdir)

    assert hash1 == hash3


def auth_variables_validate_test_cases():
    yield pytest.param(
        {
            "CDF_CLUSTER": "my_cluster",
            "CDF_PROJECT": "my_project",
            "LOGIN_FLOW": "token",
            "CDF_TOKEN": "12345",
        },
        False,
        "ok",
        [],
        {
            "cluster": "my_cluster",
            "project": "my_project",
            "cdf_url": "https://my_cluster.cognitedata.com",
            "login_flow": "token",
            "token": "12345",
            "client_id": None,
            "client_secret": None,
            "token_url": None,
            "tenant_id": None,
            "audience": "https://my_cluster.cognitedata.com",
            "scopes": "https://my_cluster.cognitedata.com/.default",
            "authority_url": None,
        },
        id="Happy path Token login",
    )

    yield pytest.param(
        {
            "CDF_CLUSTER": "my_cluster",
            "CDF_PROJECT": "my_project",
            "LOGIN_FLOW": "interactive",
            "IDP_CLIENT_ID": "7890",
            "IDP_TENANT_ID": "12345",
        },
        False,
        "ok",
        [],
        {
            "cluster": "my_cluster",
            "project": "my_project",
            "cdf_url": "https://my_cluster.cognitedata.com",
            "login_flow": "interactive",
            "token": None,
            "client_id": "7890",
            "client_secret": None,
            "token_url": "https://login.microsoftonline.com/12345/oauth2/v2.0/token",
            "tenant_id": "12345",
            "audience": "https://my_cluster.cognitedata.com",
            "scopes": "https://my_cluster.cognitedata.com/.default",
            "authority_url": "https://login.microsoftonline.com/12345",
        },
        id="Happy path Interactive login",
    )
    yield pytest.param(
        {
            "CDF_CLUSTER": "my_cluster",
            "CDF_PROJECT": "my_project",
            "LOGIN_FLOW": "client_credentials",
            "IDP_CLIENT_ID": "7890",
            "IDP_CLIENT_SECRET": "12345",
            "IDP_TENANT_ID": "12345",
        },
        False,
        "ok",
        [],
        {
            "cluster": "my_cluster",
            "project": "my_project",
            "cdf_url": "https://my_cluster.cognitedata.com",
            "login_flow": "client_credentials",
            "token": None,
            "client_id": "7890",
            "client_secret": "12345",
            "token_url": "https://login.microsoftonline.com/12345/oauth2/v2.0/token",
            "tenant_id": "12345",
            "audience": "https://my_cluster.cognitedata.com",
            "scopes": "https://my_cluster.cognitedata.com/.default",
            "authority_url": "https://login.microsoftonline.com/12345",
        },
        id="Happy path Client credentials login",
    )


class TestModuleFromPath:
    @pytest.mark.parametrize(
        "path, expected",
        [
            pytest.param(Path("cognite_modules/a_module/data_models/my_model.datamodel.yaml"), "a_module"),
            pytest.param(Path("cognite_modules/another_module/data_models/views/my_view.view.yaml"), "another_module"),
            pytest.param(
                Path("cognite_modules/parent_module/child_module/data_models/containers/my_container.container.yaml"),
                "child_module",
            ),
            pytest.param(
                Path("cognite_modules/parent_module/child_module/data_models/auth/my_group.group.yaml"), "child_module"
            ),
            pytest.param(Path("custom_modules/child_module/functions/functions.yaml"), "child_module"),
            pytest.param(Path("custom_modules/parent_module/child_module/functions/functions.yaml"), "child_module"),
        ],
    )
    def test_module_from_path(self, path: Path, expected: str):
        assert module_from_path(path) == expected


class TestIterateModules:
    def test_modules_project_for_tests(self):
        expected_modules = {
            PROJECT_FOR_TEST / "modules" / "a_module",
            PROJECT_FOR_TEST / "modules" / "another_module",
            PROJECT_FOR_TEST / "modules" / "parent_module" / "child_module",
        }

        actual_modules = {module for module, _ in iterate_modules(PROJECT_FOR_TEST)}

        assert actual_modules == expected_modules


@pytest.mark.parametrize(
    "input_, expected",
    [
        pytest.param({"a": {"b": 1, "c": 2}}, {("a", "b"): 1, ("a", "c"): 2}, id="Simple"),
        pytest.param({"a": {"b": {"c": 1}}}, {("a", "b", "c"): 1}, id="Nested"),
    ],
)
def test_flatten_dict(input_: dict[str, Any], expected: dict[str, Any]) -> None:
    actual = flatten_dict(input_)

    assert actual == expected


def quote_key_in_yaml_test_cases() -> Iterable[ParameterSet]:
    yield pytest.param(
        """space: my_space
externalID: myModel
version: 3_0_2""",
        '''space: my_space
externalID: myModel
version: "3_0_2"''',
        id="Single data model",
    )

    yield pytest.param(
        """- space: my_space
  externalId: myModel
  version: 1_000
- space: my_other_space
  externalId: myOtherModel
  version: 2_000
""",
        """- space: my_space
  externalId: myModel
  version: "1_000"
- space: my_other_space
  externalId: myOtherModel
  version: "2_000"
""",
        id="Two Data Models",
    )

    yield pytest.param(
        """space: my_space
externalID: myModel
version: '3_0_2'""",
        """space: my_space
externalID: myModel
version: '3_0_2'""",
        id="Single data model with single quoted version",
    )

    yield pytest.param(
        """- space: my_space
  externalId: myModel
  version: '1_000'
- space: my_other_space
  externalId: myOtherModel
  version: '2_000'
""",
        """- space: my_space
  externalId: myModel
  version: '1_000'
- space: my_other_space
  externalId: myOtherModel
  version: '2_000'
""",
        id="Two Data Models with single quoted version",
    )

    yield pytest.param(
        '''space: my_space
externalID: myModel
version: "3_0_2"''',
        '''space: my_space
externalID: myModel
version: "3_0_2"''',
        id="Single data model with double quoted version",
    )

    yield pytest.param(
        """- space: my_space
  externalId: myModel
  version: "1_000"
- space: my_other_space
  externalId: myOtherModel
  version: "2_000"
""",
        """- space: my_space
  externalId: myModel
  version: "1_000"
- space: my_other_space
  externalId: myOtherModel
  version: "2_000"
""",
        id="Two Data Models with double quoted version",
    )

    version_prop = """
externalId: CogniteSourceSystem
properties:
  version:
    container:
      externalId: CogniteSourceSystem
      space: sp_core_model
      type: container
    """
    yield pytest.param(
        version_prop,
        version_prop,
        id="Version property untouched",
    )


def stringify_value_by_key_in_yaml_test_cases() -> Iterable[ParameterSet]:
    yield pytest.param(
        """externalId: MyModel
config:
  data:
    debug: False
    runAll: False""",
        """externalId: MyModel
config: |
  data:
    debug: False
    runAll: False""",
        id="Stringify value under key 'config'",
    )
    input_ = """externalId: MyModel
config: |
  data:
   debug: False
   runAll: False"""
    yield pytest.param(input_, input_, id="Stringified value untouched")

    yield pytest.param(
        """config:
  data:
   debug: False
   runAll: False
externalId: MyModel""",
        """config: |
  data:
   debug: False
   runAll: False
externalId: MyModel""",
        id="Stringify value under key 'config' when it is not the first key",
    )

    input_ = """externalId: MyModel
config: another_string"""
    yield pytest.param(input_, input_, id="Stringified value untouched when it is not a dictionary")


class TestQuoteKeyInYAML:
    @pytest.mark.parametrize("raw, expected", list(quote_key_in_yaml_test_cases()))
    def test_quote_key_in_yaml(self, raw: str, expected: str) -> None:
        assert quote_int_value_by_key_in_yaml(raw, key="version") == expected

    @pytest.mark.parametrize("raw, expected", list(stringify_value_by_key_in_yaml_test_cases()))
    def test_stringify_value_by_key_in_yaml(self, raw: str, expected: str) -> None:
        actual = stringify_value_by_key_in_yaml(raw, key="config")
        assert actual == expected
        assert yaml.safe_load(actual) == yaml.safe_load(expected)


class TestModules:
    @pytest.mark.parametrize(
        "path, expected",
        [
            (Path("cdf_common/data_sets/demo.DataSet.yaml"), Path("cdf_common")),
            (Path("cdf_common/functions/contextualization_connection_writer"), Path("cdf_common")),
            (Path("sourcesystem/cdf_pi/auth/workflow.Group.yaml"), Path("sourcesystem/cdf_pi")),
        ],
    )
    def test_valid_module_directory_from_path(self, path: Path, expected: Path) -> None:
        assert module_directory_from_path(path) == expected
