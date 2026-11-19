import json
from pathlib import Path
from typing import ClassVar

import pytest
from pydantic import TypeAdapter, ValidationError

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.externaldata import (
    ExternalDataSourceRequestUnion,
    ExternalDataSourceResponseUnion,
    OneLakeCredentialsRead,
    OneLakeCredentialsWrite,
    OneLakeExternalDataSourceRequest,
    OneLakeExternalDataSourceResponse,
    OneLakeLocationDescription,
    OneLakeSettingsRead,
    OneLakeSettingsWrite,
    SnowflakeExternalDataSourceRequest,
    SnowflakeExternalDataSourceResponse,
)
from cognite_toolkit._cdf_tk.constants import MODULES
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes import ExternalDataSourceYAML
from tests.data import COMPLETE_ORG_ALPHA_FLAGS
from tests.test_unit.utils import find_resources


class TestExternalDataSourceYAML:
    VALID: ClassVar[dict[str, object]] = {
        "externalId": "fabric-lakehouse-prod",
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

    def test_load_valid_external_data_source(self) -> None:
        loaded = ExternalDataSourceYAML.model_validate(self.VALID)
        assert loaded.external_id == "fabric-lakehouse-prod"
        assert loaded.settings.credentials.client_secret is not None
        assert loaded.settings.credentials.client_secret.get_secret_value() == "azure-client-secret"

    @pytest.mark.parametrize(
        "data", list(find_resources("ExternalDataSource", base=COMPLETE_ORG_ALPHA_FLAGS / MODULES))
    )
    def test_load_valid_external_data_source_file(self, data: dict[str, object]) -> None:
        loaded = ExternalDataSourceYAML.model_validate(data)
        dumped = json.loads(loaded.model_dump_json(by_alias=True, exclude_unset=True))
        assert dumped == data

    def test_load_external_data_source_without_client_secret(self) -> None:
        data = {
            "externalId": "fabric-lakehouse-prod",
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
            ExternalDataSourceYAML.model_validate(data)

    def test_dump_client_secret(self) -> None:
        loaded = ExternalDataSourceYAML.model_validate(self.VALID)
        dumped = json.loads(loaded.model_dump_json(by_alias=True))
        assert dumped["settings"]["credentials"]["clientSecret"] == "azure-client-secret"

    def test_as_id(self) -> None:
        loaded = ExternalDataSourceYAML.model_validate(self.VALID)
        assert loaded.as_id() == ExternalId(external_id="fabric-lakehouse-prod")

    @pytest.mark.parametrize(
        "data, expected_errors",
        [
            pytest.param(
                {"externalId": "fabric-lakehouse-prod"},
                ["Missing required field: 'settings'"],
                id="missing_settings",
            ),
            pytest.param(
                {
                    "externalId": "fabric-lakehouse-prod",
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
    def test_request_default_format(self) -> None:
        request = OneLakeExternalDataSourceRequest(external_id="fabric-lakehouse-prod")
        dumped = json.loads(request.model_dump_json(by_alias=True))
        assert dumped["format"] == "one_lake"

    @pytest.mark.parametrize(
        "data, expected_cls",
        [
            pytest.param(
                {"externalId": "lakehouse", "format": "one_lake"},
                OneLakeExternalDataSourceRequest,
                id="one_lake",
            ),
            pytest.param(
                {
                    "externalId": "warehouse",
                    "format": "snowflake",
                    "expiryTime": 1,
                    "settings": {
                        "credentials": {"accountIdentifier": "acc", "userName": "user", "roleName": "role"},
                        "locationDescription": {"warehouseName": "wh"},
                    },
                },
                SnowflakeExternalDataSourceRequest,
                id="snowflake",
            ),
        ],
    )
    def test_request_union_discriminates_on_format(self, data: dict[str, object], expected_cls: type) -> None:
        loaded = TypeAdapter(ExternalDataSourceRequestUnion).validate_python(data)
        assert type(loaded) is expected_cls

    @pytest.mark.parametrize(
        "data, expected_cls",
        [
            pytest.param(
                {"externalId": "lakehouse", "format": "one_lake", "createdTime": 1, "lastUpdatedTime": 1},
                OneLakeExternalDataSourceResponse,
                id="one_lake",
            ),
            pytest.param(
                {
                    "externalId": "warehouse",
                    "format": "snowflake",
                    "createdTime": 1,
                    "lastUpdatedTime": 1,
                    "expiryTime": 1,
                    "settings": {
                        "credentials": {
                            "accountIdentifier": "acc",
                            "userName": "user",
                            "roleName": "role",
                            "publicKey": "public-key",
                        },
                        "locationDescription": {"warehouseName": "wh"},
                    },
                },
                SnowflakeExternalDataSourceResponse,
                id="snowflake",
            ),
        ],
    )
    def test_response_union_discriminates_on_format(self, data: dict[str, object], expected_cls: type) -> None:
        loaded = TypeAdapter(ExternalDataSourceResponseUnion).validate_python(data)
        assert type(loaded) is expected_cls

    def test_response_union_rejects_invalid_format(self) -> None:
        with pytest.raises(ValidationError):
            TypeAdapter(ExternalDataSourceResponseUnion).validate_python(
                {"externalId": "fabric-lakehouse-prod", "format": "invalid", "createdTime": 1, "lastUpdatedTime": 1}
            )

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
