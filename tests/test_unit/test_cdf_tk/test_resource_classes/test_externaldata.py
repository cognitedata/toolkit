import json
from pathlib import Path
from typing import ClassVar

import pytest
from pydantic import ValidationError

from cognite_toolkit._cdf_tk.client.identifiers import ExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.externaldata import (
    ExternalDataSourceRequest,
    ExternalDataSourceResponse,
    OneLakeCredentialsRead,
    OneLakeCredentialsWrite,
    OneLakeLocationDescription,
    OneLakeSettingsRead,
    OneLakeSettingsWrite,
)
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.validation import validate_resource_yaml_pydantic
from cognite_toolkit._cdf_tk.yaml_classes import ExternalDataSourceYAML


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
        loaded = ExternalDataSourceYAML.model_validate(data)
        assert loaded.settings.credentials.client_secret is None

    def test_dump_client_secret(self) -> None:
        loaded = ExternalDataSourceYAML.model_validate(self.VALID)
        dumped = json.loads(loaded.model_dump_json(by_alias=True))
        assert dumped["settings"]["credentials"]["clientSecret"] == "azure-client-secret"

    def test_dump_client_secret_none(self) -> None:
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
        loaded = ExternalDataSourceYAML.model_validate(data)
        dumped = json.loads(loaded.model_dump_json(by_alias=True))
        assert dumped["settings"]["credentials"].get("clientSecret") is None

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
        request = ExternalDataSourceRequest(external_id="fabric-lakehouse-prod")
        assert request.format == "one_lake"
        dumped = json.loads(request.model_dump_json(by_alias=True))
        assert dumped["format"] == "one_lake"

    def test_request_rejects_invalid_format(self) -> None:
        with pytest.raises(ValidationError):
            ExternalDataSourceRequest(external_id="fabric-lakehouse-prod", format="invalid")

    def test_request_as_id(self) -> None:
        request = ExternalDataSourceRequest(
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
        response = ExternalDataSourceResponse(
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
