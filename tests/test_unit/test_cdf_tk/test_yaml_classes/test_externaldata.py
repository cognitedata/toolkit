import json
from pathlib import Path
from typing import ClassVar

import pytest
from pydantic import TypeAdapter, ValidationError

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.externaldata import (
    ExternalDataSourceRequestAdapter,
    ExternalDataSourceResponseAdapter,
    OneLakeCredentialsRead,
    OneLakeCredentialsWrite,
    OneLakeExternalDataSourceRequest,
    OneLakeExternalDataSourceResponse,
    OneLakeLocationDescription,
    OneLakeSettingsRead,
    OneLakeSettingsWrite,
    SnowflakeCredentialsRead,
    SnowflakeCredentialsWrite,
    SnowflakeExternalDataSourceRequest,
    SnowflakeExternalDataSourceResponse,
    SnowflakeLocationDescription,
    SnowflakeSettingsRead,
    SnowflakeSettingsWrite,
    UnknownExternalDataSourceRequest,
    UnknownExternalDataSourceResponse,
)
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes import (
    ExternalDataSourceYAML,
    OneLakeExternalDataSourceYAML,
    SnowflakeExternalDataSourceYAML,
)
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources

_YAML = TypeAdapter(ExternalDataSourceYAML)


class TestExternalDataSourceYAML:
    VALID: ClassVar[dict[str, object]] = {
        "externalId": "fabric-lakehouse-prod",
        "format": "one_lake",
        "name": "Production lakehouse",
        "dataSetExternalId": "my_dataset",
        "settings": {
            "credentials": {
                "clientId": "azure-client-id",
                "tenantId": "azure-tenant-id",
                "clientSecret": "azure-client-secret",
            },
            "locationDescription": {
                "workspaceId": "workspace-guid",
                "containerId": "lakehouse-guid",
            },
        },
    }
    VALID_SNOWFLAKE: ClassVar[dict[str, object]] = {
        "externalId": "snowflake-prod",
        "format": "snowflake",
        "expiryTime": 2_000_000_000_000,
        "settings": {
            "credentials": {
                "accountIdentifier": "myorg-myaccount",
                "userName": "cdf_user",
                "roleName": "CDF_ROLE",
            },
            "locationDescription": {"warehouseName": "CDF_WH"},
        },
    }

    def test_load_valid_external_data_source(self) -> None:
        loaded = _YAML.validate_python(self.VALID)
        assert isinstance(loaded, OneLakeExternalDataSourceYAML)
        assert loaded.external_id == "fabric-lakehouse-prod"
        assert loaded.settings.credentials.client_secret is not None
        assert loaded.settings.credentials.client_secret.get_secret_value() == "azure-client-secret"

    @pytest.mark.parametrize(
        "data", list(find_resources("ExternalDataSource", base=COMPLETE_ORG_ALPHA_FLAGS / MODULES))
    )
    def test_load_valid_external_data_source_file(self, data: dict[str, object]) -> None:
        loaded = _YAML.validate_python(data)
        dumped = json.loads(loaded.model_dump_json(by_alias=True, exclude_unset=True))
        assert dumped == data

    def test_load_external_data_source_without_client_secret(self) -> None:
        data = {
            "externalId": "fabric-lakehouse-prod",
            "format": "one_lake",
            "settings": {
                "credentials": {
                    "clientId": "azure-client-id",
                    "tenantId": "azure-tenant-id",
                },
                "locationDescription": {
                    "workspaceId": "workspace-guid",
                    "containerId": "lakehouse-guid",
                },
            },
        }
        with pytest.raises(ValidationError):
            _YAML.validate_python(data)

    def test_load_valid_snowflake_external_data_source(self) -> None:
        loaded = _YAML.validate_python(self.VALID_SNOWFLAKE)
        assert isinstance(loaded, SnowflakeExternalDataSourceYAML)
        assert loaded.format == "snowflake"
        assert loaded.expiry_time == 2_000_000_000_000
        assert loaded.settings.credentials.account_identifier == "myorg-myaccount"

    @pytest.mark.parametrize(
        "field",
        ["expiryTime", "warehouseName"],
    )
    def test_snowflake_requires_expiry_and_warehouse(self, field: str) -> None:
        data = json.loads(json.dumps(self.VALID_SNOWFLAKE))
        if field == "expiryTime":
            data.pop(field)
        else:
            data["settings"]["locationDescription"].pop(field)
        with pytest.raises(ValidationError):
            _YAML.validate_python(data)

    def test_dump_client_secret(self) -> None:
        loaded = _YAML.validate_python(self.VALID)
        dumped = json.loads(loaded.model_dump_json(by_alias=True))
        assert dumped["settings"]["credentials"]["clientSecret"] == "azure-client-secret"

    def test_as_id(self) -> None:
        loaded = _YAML.validate_python(self.VALID)
        assert loaded.as_id() == ExternalId(external_id="fabric-lakehouse-prod")

    @pytest.mark.parametrize(
        "data, expected_errors",
        [
            pytest.param(
                {"externalId": "fabric-lakehouse-prod", "settings": {}},
                ["Missing required field: 'format'"],
                id="missing_format",
            ),
            pytest.param(
                {"externalId": "fabric-lakehouse-prod", "format": "one_lake"},
                ["Missing required field: 'settings'"],
                id="missing_settings",
            ),
            pytest.param(
                {
                    "externalId": "fabric-lakehouse-prod",
                    "format": "one_lake",
                    "settings": {
                        "credentials": {
                            "clientId": "azure-client-id",
                            "tenantId": "azure-tenant-id",
                        },
                        "locationDescription": {
                            "workspaceId": "workspace-guid",
                            "containerId": "lakehouse-guid",
                        },
                    },
                },
                ["Missing required field in settings.credentials: 'clientSecret'"],
                id="missing_client_secret",
            ),
        ],
    )
    def test_load_invalid_external_data_source(self, data: dict[str, object], expected_errors: list[str]) -> None:
        warnings = validate_resource_yaml_pydantic(data, ExternalDataSourceYAML, source_file=Path("test.yaml"))
        assert len(warnings) == 1
        warning = warnings[0]
        assert isinstance(warning, ResourceFormatWarning)
        assert list(warning.errors) == expected_errors


class TestExternalDataSourceResourceClasses:
    def test_request_requires_format(self) -> None:
        with pytest.raises(ValidationError, match="format"):
            ExternalDataSourceRequestAdapter.validate_python({"externalId": "fabric-lakehouse-prod"})

    def test_request_unknown_format(self) -> None:
        request = ExternalDataSourceRequestAdapter.validate_python(
            {"externalId": "fabric-lakehouse-prod", "format": "databricks"}
        )
        assert isinstance(request, UnknownExternalDataSourceRequest)
        assert request.format == "databricks"

    def test_write_credentials_require_client_secret(self) -> None:
        with pytest.raises(ValidationError):
            OneLakeCredentialsWrite.model_validate(
                {
                    "clientId": "id",
                    "tenantId": "tenant",
                }
            )

    def test_request_as_id(self) -> None:
        request = OneLakeExternalDataSourceRequest(
            external_id="fabric-lakehouse-prod",
            settings=OneLakeSettingsWrite(
                credentials=OneLakeCredentialsWrite(
                    client_id="id",
                    tenant_id="tenant",
                    client_secret="secret",
                ),
                location_description=OneLakeLocationDescription(
                    workspace_id="workspace-guid",
                    container_id="lakehouse-guid",
                ),
            ),
        )
        assert request.as_id() == ExternalId(external_id="fabric-lakehouse-prod")

    def test_snowflake_request(self) -> None:
        request = SnowflakeExternalDataSourceRequest(
            external_id="snowflake-prod",
            expiry_time=2_000_000_000_000,
            settings=SnowflakeSettingsWrite(
                credentials=SnowflakeCredentialsWrite(
                    account_identifier="myorg-myaccount",
                    user_name="cdf_user",
                    role_name="CDF_ROLE",
                ),
                location_description=SnowflakeLocationDescription(warehouse_name="CDF_WH"),
            ),
        )
        assert request.dump()["settings"]["credentials"] == {
            "accountIdentifier": "myorg-myaccount",
            "userName": "cdf_user",
            "roleName": "CDF_ROLE",
        }

    def test_snowflake_request_requires_expiry_time(self) -> None:
        with pytest.raises(ValidationError, match="expiryTime"):
            SnowflakeExternalDataSourceRequest.model_validate(
                {
                    "externalId": "snowflake-prod",
                    "settings": {
                        "credentials": {
                            "accountIdentifier": "myorg-myaccount",
                            "userName": "cdf_user",
                            "roleName": "CDF_ROLE",
                        },
                        "locationDescription": {"warehouseName": "CDF_WH"},
                    },
                }
            )

    def test_response_default_format(self) -> None:
        response = OneLakeExternalDataSourceResponse(
            external_id="fabric-lakehouse-prod",
            created_time=1,
            last_updated_time=1,
        )
        assert response.format == "one_lake"

    def test_response_unknown_format(self) -> None:
        response = ExternalDataSourceResponseAdapter.validate_python(
            {
                "externalId": "fabric-lakehouse-prod",
                "format": "databricks",
                "createdTime": 1,
                "lastUpdatedTime": 1,
                "settings": {"host": "example"},
            }
        )
        assert isinstance(response, UnknownExternalDataSourceResponse)
        assert response.as_request_resource().dump() == {
            "externalId": "fabric-lakehouse-prod",
            "format": "databricks",
            "settings": {"host": "example"},
        }

    def test_response_as_id(self) -> None:
        response = OneLakeExternalDataSourceResponse(
            external_id="fabric-lakehouse-prod",
            format="one_lake",
            created_time=1,
            last_updated_time=1,
            settings=OneLakeSettingsRead(
                credentials=OneLakeCredentialsRead(client_id="id", tenant_id="tenant"),
                location_description=OneLakeLocationDescription(
                    workspace_id="workspace-guid",
                    container_id="lakehouse-guid",
                ),
            ),
        )
        assert response.as_id() == ExternalId(external_id="fabric-lakehouse-prod")

    def test_snowflake_response(self) -> None:
        response = SnowflakeExternalDataSourceResponse(
            external_id="snowflake-prod",
            expiry_time=2_000_000_000_000,
            created_time=1,
            last_updated_time=2,
            settings=SnowflakeSettingsRead(
                credentials=SnowflakeCredentialsRead(
                    account_identifier="myorg-myaccount",
                    user_name="cdf_user",
                    role_name="CDF_ROLE",
                    public_key="public-key",
                ),
                location_description=SnowflakeLocationDescription(warehouse_name="CDF_WH"),
            ),
        )
        assert response.settings.credentials.public_key == "public-key"
