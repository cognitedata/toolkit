from pathlib import Path
from typing import ClassVar

import pytest

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
                "workspaceName": "workspace-guid",
                "containerName": "lakehouse-guid",
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
                    "workspaceName": "workspace-guid",
                    "containerName": "lakehouse-guid",
                },
            },
        }
        loaded = ExternalDataSourceYAML.model_validate(data)
        assert loaded.settings.credentials.client_secret is None

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
