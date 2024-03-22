import shutil
import tempfile
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest
import yaml
from cognite.client._api.iam import TokenAPI, TokenInspection
from cognite.client.credentials import OAuthClientCredentials, OAuthInteractive
from cognite.client.data_classes import TimeSeries
from cognite.client.data_classes.capabilities import (
    DataSetsAcl,
    ProjectCapability,
    ProjectCapabilityList,
    ProjectsScope,
)
from cognite.client.data_classes.data_modeling import ViewApply
from cognite.client.data_classes.iam import ProjectSpec
from cognite.client.exceptions import CogniteAuthError
from cognite.client.testing import CogniteClientMock, monkeypatch_cognite_client
from pytest import MonkeyPatch

from cognite_toolkit._cdf_tk.utils import (
    AuthVariables,
    CDFToolConfig,
    DataSetMissingWarning,
    SnakeCaseWarning,
    TemplateVariableWarning,
    calculate_directory_hash,
    load_yaml_inject_variables,
    validate_case_raw,
    validate_data_set_is_set,
    validate_modules_variables,
)

THIS_FOLDER = Path(__file__).resolve().parent

DATA_FOLDER = THIS_FOLDER / "load_data"


def mocked_init(self):
    self._client = CogniteClientMock()
    self._cache = CDFToolConfig._Cache()


def test_init():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        instance = CDFToolConfig()
        assert isinstance(instance._client, CogniteClientMock)


@pytest.mark.skip("Rewrite to use ApprovalClient")
def test_dataset_missing_acl():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        with pytest.raises(CogniteAuthError):
            instance = CDFToolConfig()
            instance.verify_dataset("test")


def test_dataset_create():
    with patch.object(CDFToolConfig, "__init__", mocked_init):
        instance = CDFToolConfig()
        instance._client.config.project = "cdf-project-templates"
        instance._client.iam.token.inspect = Mock(
            spec=TokenAPI.inspect,
            return_value=TokenInspection(
                subject="",
                capabilities=ProjectCapabilityList(
                    [
                        ProjectCapability(
                            capability=DataSetsAcl(
                                [DataSetsAcl.Action.Read, DataSetsAcl.Action.Write], scope=DataSetsAcl.Scope.All()
                            ),
                            project_scope=ProjectsScope(["cdf-project-templates"]),
                        )
                    ],
                    cognite_client=instance._client,
                ),
                projects=[ProjectSpec(url_name="cdf-project-templates", groups=[])],
            ),
        )

        # the dataset exists
        instance.verify_dataset("test")
        assert instance._client.data_sets.retrieve.call_count == 1


def test_load_yaml_inject_variables(tmp_path) -> None:
    my_file = tmp_path / "test.yaml"
    my_file.write_text(yaml.safe_dump({"test": "${TEST}"}))

    loaded = load_yaml_inject_variables(my_file, {"TEST": "my_injected_value"})

    assert loaded["test"] == "my_injected_value"


def test_validate_raw() -> None:
    raw_file = DATA_FOLDER / "timeseries" / "wrong_case.yaml"

    warnings = validate_case_raw(yaml.safe_load(raw_file.read_text()), TimeSeries, raw_file)

    assert len(warnings) == 2
    assert sorted(warnings) == sorted(
        [
            SnakeCaseWarning(raw_file, "wrong_case", "externalId", "is_string", "isString"),
            SnakeCaseWarning(raw_file, "wrong_case", "externalId", "is_step", "isStep"),
        ]
    )


def test_validate_raw_nested() -> None:
    raw_file = DATA_FOLDER / "datamodels" / "snake_cased_view_property.yaml"
    warnings = validate_case_raw(yaml.safe_load(raw_file.read_text()), ViewApply, raw_file)

    assert len(warnings) == 1
    assert warnings == [
        SnakeCaseWarning(
            raw_file, "WorkItem", "externalId", "container_property_identifier", "containerPropertyIdentifier"
        )
    ]


@pytest.mark.parametrize(
    "config_yaml, expected_warnings",
    [
        pytest.param(
            {"sourceId": "<change_me>"},
            [TemplateVariableWarning(Path("config.yaml"), "<change_me>", "sourceId", "")],
            id="Single warning",
        ),
        pytest.param(
            {"a_module": {"sourceId": "<change_me>"}},
            [TemplateVariableWarning(Path("config.yaml"), "<change_me>", "sourceId", "a_module")],
            id="Nested warning",
        ),
        pytest.param(
            {"a_super_module": {"a_module": {"sourceId": "<change_me>"}}},
            [TemplateVariableWarning(Path("config.yaml"), "<change_me>", "sourceId", "a_super_module.a_module")],
            id="Deep nested warning",
        ),
        pytest.param({"a_module": {"sourceId": "123"}}, [], id="No warning"),
    ],
)
def test_validate_config_yaml(config_yaml: dict[str, Any], expected_warnings: list[TemplateVariableWarning]) -> None:
    warnings = validate_modules_variables(config_yaml, Path("config.yaml"))

    assert sorted(warnings) == sorted(expected_warnings)


def test_validate_data_set_is_set():
    warnings = validate_data_set_is_set(
        {"externalId": "myTimeSeries", "name": "My Time Series"}, TimeSeries, Path("timeseries.yaml")
    )

    assert sorted(warnings) == sorted(
        [DataSetMissingWarning(Path("timeseries.yaml"), "myTimeSeries", "externalId", "TimeSeries")]
    )


def test_calculate_hash_on_folder():
    folder = Path(THIS_FOLDER / "calc_hash_data")
    hash1 = calculate_directory_hash(folder)
    hash2 = calculate_directory_hash(folder)

    print(hash1)

    assert (
        hash1 == "e60120ed03ebc1de314222a6a330dce08b7e2d77ec0929cd3c603cfdc08999ad"
    ), f"The hash should not change as long as content in {folder} is not changed."
    assert hash1 == hash2
    tempdir = Path(tempfile.mkdtemp())
    shutil.rmtree(tempdir)
    shutil.copytree(folder, tempdir)
    hash3 = calculate_directory_hash(tempdir)
    shutil.rmtree(tempdir)

    assert hash1 == hash3


class TestCDFToolConfig:
    def test_initialize_token(self):
        expected = """# .env file generated by cognite-toolkit
CDF_CLUSTER=my_cluster
CDF_PROJECT=my_project
LOGIN_FLOW=token
# When using a token, the IDP variables are not needed, so they are not included.
CDF_TOKEN=12345
# The below variables are the defaults, they are automatically constructed unless they are set.
CDF_URL=https://my_cluster.cognitedata.com"""
        with monkeypatch_cognite_client() as _:
            config = CDFToolConfig(token="12345", cluster="my_cluster", project="my_project")
            env_file = AuthVariables.from_env(config._environ).create_dotenv_file()
        assert env_file.splitlines() == expected.splitlines()

    def test_initialize_interactive_login(self):
        expected = """# .env file generated by cognite-toolkit
CDF_CLUSTER=my_cluster
CDF_PROJECT=my_project
LOGIN_FLOW=interactive
IDP_CLIENT_ID=7890
# Note: Either the TENANT_ID or the TENANT_URL must be written.
IDP_TENANT_ID=12345
IDP_TOKEN_URL=https://login.microsoftonline.com/12345/oauth2/v2.0/token
# The below variables are the defaults, they are automatically constructed unless they are set.
CDF_URL=https://my_cluster.cognitedata.com
IDP_SCOPES=https://my_cluster.cognitedata.com/.default
IDP_AUTHORITY_URL=https://login.microsoftonline.com/12345"""
        with MonkeyPatch.context() as mp:
            mp.setenv("LOGIN_FLOW", "interactive")
            mp.setenv("CDF_CLUSTER", "my_cluster")
            mp.setenv("CDF_PROJECT", "my_project")
            mp.setenv("IDP_TENANT_ID", "12345")
            mp.setenv("IDP_CLIENT_ID", "7890")
            mp.setattr("cognite_toolkit._cdf_tk.utils.OAuthInteractive", MagicMock(spec=OAuthInteractive))
            with monkeypatch_cognite_client() as _:
                config = CDFToolConfig()
                env_file = AuthVariables.from_env(config._environ).create_dotenv_file()
        assert env_file.splitlines() == expected.splitlines()

    def test_initialize_client_credentials_login(self):
        expected = """# .env file generated by cognite-toolkit
CDF_CLUSTER=my_cluster
CDF_PROJECT=my_project
LOGIN_FLOW=client_credentials
IDP_CLIENT_ID=7890
IDP_CLIENT_SECRET=12345
# Note: Either the TENANT_ID or the TENANT_URL must be written.
IDP_TENANT_ID=12345
IDP_TOKEN_URL=https://login.microsoftonline.com/12345/oauth2/v2.0/token
# The below variables are the defaults, they are automatically constructed unless they are set.
CDF_URL=https://my_cluster.cognitedata.com
IDP_SCOPES=https://my_cluster.cognitedata.com/.default
IDP_AUDIENCE=https://my_cluster.cognitedata.com"""
        with MonkeyPatch.context() as mp:
            mp.setenv("LOGIN_FLOW", "client_credentials")
            mp.setenv("CDF_CLUSTER", "my_cluster")
            mp.setenv("CDF_PROJECT", "my_project")
            mp.setenv("IDP_TENANT_ID", "12345")
            mp.setenv("IDP_CLIENT_ID", "7890")
            mp.setenv("IDP_CLIENT_SECRET", "12345")
            mp.setattr("cognite_toolkit._cdf_tk.utils.OAuthClientCredentials", MagicMock(spec=OAuthClientCredentials))
            with monkeypatch_cognite_client() as _:
                config = CDFToolConfig()
                env_file = AuthVariables.from_env(config._environ).create_dotenv_file()
        assert env_file.splitlines() == expected.splitlines()


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

    yield pytest.param(
        {
            "CDF_CLUSTER": "my_cluster",
        },
        False,
        "error",
        ["  CDF project URL name is not set.", "  [bold red]ERROR[/]: CDF Cluster and project are required."],
        {},
        id="Missing project",
    )


class TestAuthVariables:
    @pytest.mark.parametrize(
        "environment_variables, verbose, expected_status, expected_messages, expected_vars",
        auth_variables_validate_test_cases(),
    )
    def test_validate(
        self,
        environment_variables: dict[str, str],
        verbose: bool,
        expected_status: str,
        expected_messages: list[str],
        expected_vars: dict[str, str],
    ) -> None:
        with MonkeyPatch.context() as mp:
            for key, value in environment_variables.items():
                mp.setenv(key, value)
            auth_var = AuthVariables.from_env()
            results = auth_var.validate(verbose)

            assert results.status == expected_status
            assert results.messages == expected_messages

            if expected_vars:
                assert vars(auth_var) == expected_vars
