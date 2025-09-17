from pathlib import Path
from unittest.mock import MagicMock

import pytest
import responses
import yaml
from cognite.client.credentials import OAuthClientCredentials
from cognite.client.data_classes import (
    ClientCredentials,
    Function,
    FunctionSchedule,
    FunctionScheduleWrite,
    FunctionScheduleWriteList,
    FunctionWrite,
)
from cognite.client.data_classes.capabilities import FilesAcl, FunctionsAcl
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient, ToolkitClientConfig
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.cruds import FunctionCRUD, FunctionScheduleCRUD, ResourceWorker
from cognite_toolkit._cdf_tk.exceptions import ResourceCreationError, ToolkitRequiredValueError
from cognite_toolkit._cdf_tk.utils import calculate_directory_hash, calculate_secure_hash
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from tests.data import LOAD_DATA
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestFunctionLoader:
    def test_load_functions(self, env_vars_with_client: EnvironmentVariables) -> None:
        loader = FunctionCRUD.create_loader(env_vars_with_client.get_client(), LOAD_DATA)

        raw_list = loader.load_resource_file(
            LOAD_DATA / "functions" / "1.my_functions.yaml", env_vars_with_client.dump()
        )

        assert len(raw_list) == 2

    def test_load_function(self, env_vars_with_client: EnvironmentVariables) -> None:
        loader = FunctionCRUD.create_loader(env_vars_with_client.get_client(), LOAD_DATA)

        raw_list = loader.load_resource_file(
            LOAD_DATA / "functions" / "1.my_function.yaml", env_vars_with_client.dump()
        )
        loaded = loader.load_resource(raw_list[0], is_dry_run=False)

        assert isinstance(loaded, FunctionWrite)

    def test_update_secrets(
        self, env_vars_with_client: EnvironmentVariables, toolkit_client_approval: ApprovalToolkitClient, tmp_path: Path
    ) -> None:
        local_yaml = """name: my_function
externalId: my_function
secrets:
    secret1: value1
    secret2: value2
        """
        cdf_function = Function(
            name="my_function",
            external_id="my_function",
            file_id=123,
            status="Ready",
            metadata={
                FunctionCRUD._MetadataKey.function_hash: FunctionCRUD._create_hash_values(tmp_path / "my_function"),
                FunctionCRUD._MetadataKey.secret_hash: calculate_secure_hash(
                    {
                        "secret1": "value1",
                        "secret2": "value2",
                    }
                ),
            },
            secrets={
                # The API returns secrets masked
                "secret1": "***",
                "secret2": "***",
            },
        )
        toolkit_client_approval.append(Function, cdf_function)

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_yaml
        filepath.parent.name = FunctionCRUD.folder_name

        worker = ResourceWorker(FunctionCRUD.create_loader(env_vars_with_client.get_client(), tmp_path), "deploy")
        resources = worker.prepare_resources([filepath])

        assert {
            "create": len(resources.to_create),
            "update": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "update": 0, "delete": 0, "unchanged": 1}

        toolkit_client_approval.clear_cdf_resources(Function)
        cdf_function.metadata[FunctionCRUD._MetadataKey.secret_hash] = calculate_secure_hash(
            {
                "secret1": "value1",
                "secret2": "updated_value2",
            }
        )
        toolkit_client_approval.append(Function, cdf_function)
        resources = worker.prepare_resources([filepath])

        assert {
            "create": len(resources.to_create),
            "update": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 1, "update": 0, "delete": 1, "unchanged": 0}

    def test_dump_index_url_set(self, env_vars_with_client: EnvironmentVariables, tmp_path: Path) -> None:
        local_dict = FunctionWrite(
            name="my_function",
            file_id=123,
            external_id="my_function",
            index_url="http://my-index-url",
        ).dump()
        cdf_function = Function(
            name="my_function",
            file_id=123,
            external_id="my_function",
            metadata={
                FunctionCRUD._MetadataKey.function_hash: calculate_directory_hash(
                    tmp_path / "my_function", exclude_prefixes={".DS_Store"}
                ),
            },
        )
        loader = FunctionCRUD.create_loader(env_vars_with_client.get_client(), tmp_path)

        dumped = loader.dump_resource(cdf_function, local_dict)

        assert "indexUrl" in dumped
        assert dumped["indexUrl"] == "http://my-index-url"

    def test_get_function_required_capabilities(self, env_vars_with_client: EnvironmentVariables) -> None:
        loader = FunctionCRUD.create_loader(env_vars_with_client.get_client(), None)
        loader.data_set_id_by_external_id = {"function1": 123, "function2": 456}

        # Mock data
        items = [
            FunctionWrite(external_id="function1", name="Function 1", file_id=1001),
            FunctionWrite(external_id="function2", name="Function 2", file_id=1002),
            FunctionWrite(external_id="function3", name="Function 3", file_id=1003),
        ]

        # Test read-only mode
        read_capabilities = loader.get_function_required_capabilities(items, read_only=True)
        assert len(read_capabilities) == 2
        assert isinstance(read_capabilities[0], FunctionsAcl)
        assert isinstance(read_capabilities[1], FilesAcl)
        assert read_capabilities[0].actions == [FunctionsAcl.Action.Read]
        assert read_capabilities[1].actions == [FilesAcl.Action.Read]
        assert isinstance(read_capabilities[1].scope, FilesAcl.Scope.DataSet)
        assert sorted(read_capabilities[1].scope.ids) == [123, 456]

        # Test write mode
        write_capabilities = loader.get_function_required_capabilities(items, read_only=False)
        assert len(write_capabilities) == 2
        assert write_capabilities[0].actions == [FunctionsAcl.Action.Read, FunctionsAcl.Action.Write]
        assert write_capabilities[1].actions == [FilesAcl.Action.Read, FilesAcl.Action.Write]
        assert isinstance(write_capabilities[1].scope, FilesAcl.Scope.DataSet)
        assert sorted(write_capabilities[1].scope.ids) == [123, 456]

    def test_get_function_required_capabilities_empty(self, env_vars_with_client: EnvironmentVariables) -> None:
        loader = FunctionCRUD.create_loader(env_vars_with_client.get_client(), None)
        capabilities = loader.get_function_required_capabilities([], read_only=False)
        assert capabilities == []

    def test_get_function_required_capabilities_no_datasets(self, env_vars_with_client: EnvironmentVariables) -> None:
        loader = FunctionCRUD.create_loader(env_vars_with_client.get_client(), None)
        items = [
            FunctionWrite(external_id="function1", name="Function 1", file_id=1001),
            FunctionWrite(external_id="function2", name="Function 2", file_id=1002),
        ]

        capabilities = loader.get_function_required_capabilities(items, read_only=False)
        assert len(capabilities) == 2
        assert isinstance(capabilities[1].scope, FilesAcl.Scope.All)


class TestFunctionScheduleLoader:
    def test_credentials_missing_raise(self) -> None:
        schedule = dict(
            name="daily-8am-utc",
            functionExternalId="fn_example_repeater",
            cronExpression="0 8 * * *",
        )
        config = MagicMock(spec=ToolkitClientConfig)
        config.is_strict_validation = True
        config.credentials = OAuthClientCredentials(
            client_id="toolkit-client-id",
            client_secret="toolkit-client-secret",
            token_url="https://cognite.com/token",
            scopes=["USER_IMPERSONATION"],
        )
        with monkeypatch_toolkit_client() as client:
            client.config = config
            loader = FunctionScheduleCRUD.create_loader(client)

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = yaml.dump(schedule)
        with pytest.raises(ToolkitRequiredValueError):
            loader.load_resource_file(filepath, {})
        client.config.is_strict_validation = False
        filepath.read_text.return_value = yaml.dump(schedule)
        local = loader.load_resource_file(filepath, {})[0]
        credentials = loader.authentication_by_id[loader.get_id(local)]
        assert credentials.client_id == "toolkit-client-id"
        assert credentials.client_secret == "toolkit-client-secret"

    def test_credentials_unchanged_changed(self) -> None:
        local_content = """name: daily-8am-utc
functionExternalId: fn_example_repeater
cronExpression: 0 8 * * *
description: Run the function every day at 8am UTC
authentication:
  clientId: my-client-id
  clientSecret: my-client-secret
"""
        auth_dict = yaml.CSafeLoader(local_content).get_data()["authentication"]
        auth_hash = calculate_secure_hash(auth_dict, shorten=True)

        with monkeypatch_toolkit_client() as client:
            cdf_schedule = FunctionSchedule(
                id=123,
                name="daily-8am-utc",
                function_external_id="fn_example_repeater",
                cron_expression="0 8 * * *",
                description=f"Run the function every day at 8am UTC {FunctionScheduleCRUD._hash_key}: {auth_hash}",
                cognite_client=client,
            )
            # The as_write method looks up the input data.
            client.functions.schedules.get_input_data.return_value = None
            loader = FunctionScheduleCRUD(client, None, None)

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_content
        local_dumped = loader.load_resource_file(filepath, {})[0]
        cdf_dumped = loader.dump_resource(cdf_schedule, local_dumped)

        assert cdf_dumped == local_dumped

        filepath = MagicMock(spec=Path)
        filepath.read_text.return_value = local_content.replace("my-client-secret", "my-client-secret-changed")
        local_dumped = loader.load_resource_file(filepath, {})[0]
        cdf_dumped = loader.dump_resource(cdf_schedule, local_dumped)

        assert cdf_dumped != local_dumped

    @pytest.mark.parametrize(
        "client_id,client_secret, expected_error",
        [
            pytest.param(
                "invalid-client-id",
                "invalid-client-secret",
                CogniteAPIError(message="Invalid client credentials", code=400),
                id="Invalid credentials no extra hint",
            ),
            pytest.param(
                "123789109801",
                "${MY_SUPER_SECRET}",
                ResourceCreationError(
                    "Failed to create Function Schedule FunctionScheduleID(function_external_id='fn_example_repeater', name='daily-8am-utc'): The environment variable is not set: MY_SUPER_SECRET"
                ),
                id="Invalid credentials missing envrionment variable",
            ),
        ],
    )
    def test_create_error_message_invalid_credentials(
        self, client_id: str, client_secret: str, expected_error: Exception, toolkit_config: ToolkitClientConfig
    ) -> None:
        config = toolkit_config
        with responses.RequestsMock() as rsps:
            rsps.add(
                rsps.POST,
                config.create_api_url("functions/byids"),
                status=200,
                json={
                    "items": [
                        {
                            "id": 1,
                            "name": "fn_example_repeater",
                            "externalId": "fn_example_repeater",
                            "status": "Ready",
                            "fileId": 101,
                        }
                    ]
                },
            )
            rsps.add(
                rsps.POST,
                config.create_api_url("/sessions"),
                status=400,
                json={"error": {"message": "Invalid client credentials"}},
            )
            client = ToolkitClient(toolkit_config)
            loader = FunctionScheduleCRUD(client, None, None)
            schedule = FunctionScheduleWrite(
                name="daily-8am-utc",
                function_external_id="fn_example_repeater",
                cron_expression="0 8 * * *",
            )
            id_ = loader.get_id(schedule)
            loader.authentication_by_id[id_] = ClientCredentials(
                client_id=client_id,
                client_secret=client_secret,
            )
            with pytest.raises(type(expected_error)) as exc_val:
                loader.create(FunctionScheduleWriteList([schedule]))

            assert str(expected_error) in str(exc_val.value)

    def test_create_function_schedule_missing_function(self, toolkit_config: ToolkitClientConfig) -> None:
        config = toolkit_config
        with responses.RequestsMock() as rsps:
            rsps.add(
                rsps.POST,
                config.create_api_url("functions/byids"),
                status=200,
                json={"items": []},
            )
            client = ToolkitClient(toolkit_config)
            loader = FunctionScheduleCRUD(client, None, None)
            schedule = FunctionScheduleWrite(
                name="daily-8am-utc",
                function_external_id="fn_non_existent_function",
                cron_expression="0 8 * * *",
            )
            with pytest.raises(ResourceCreationError) as exc_val:
                loader.create(FunctionScheduleWriteList([schedule]))

            assert (
                "Failed to create function schedule(s) "
                "FunctionScheduleID(function_external_id='fn_non_existent_function', name='daily-8am-utc'). "
                "Could not find function(s) 'fn_non_existent_function'"
            ) in str(exc_val.value)
