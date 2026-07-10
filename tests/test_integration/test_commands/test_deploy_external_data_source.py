import os
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client import ToolkitClient

_JETFIRE_ENV = Path(__file__).parents[4] / "jetfire-backend" / ".env"
_FABRIC_ENV_VARS = (
    "FABRIC_CLIENT_ID",
    "FABRIC_TENANT_ID",
    "FABRIC_CLIENT_SECRET",
    "FABRIC_WORKSPACE",
    "FABRIC_LAKEHOUSE",
)


def _load_jetfire_env() -> None:
    if not _JETFIRE_ENV.is_file():
        return
    for line in _JETFIRE_ENV.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())


def _fabric_ci_available() -> bool:
    _load_jetfire_env()
    return all(os.environ.get(key) for key in _FABRIC_ENV_VARS)


@pytest.mark.skipif(not _fabric_ci_available(), reason="Fabric integration credentials not available in environment")
class TestDeployExternalDataSourceIntegration:
    def test_external_data_source_lifecycle(self, toolkit_client: ToolkitClient) -> None:
        from cognite_toolkit._cdf_tk.client.resource_classes.external_data_source import (
            ExternalDataSourceRequest,
            OneLakeCredentialsWrite,
            OneLakeLocationDescription,
            OneLakeSettingsWrite,
        )

        external_id = "toolkit-integration-fabric-onelake"
        source = ExternalDataSourceRequest(
            external_id=external_id,
            name="Toolkit integration test",
            settings=OneLakeSettingsWrite(
                credentials=OneLakeCredentialsWrite(
                    client_id=os.environ["FABRIC_CLIENT_ID"],
                    tenant_id=os.environ["FABRIC_TENANT_ID"],
                    client_secret=os.environ["FABRIC_CLIENT_SECRET"],
                ),
                location_description=OneLakeLocationDescription(
                    workspace_name=os.environ["FABRIC_WORKSPACE"],
                    container_name=os.environ["FABRIC_LAKEHOUSE"],
                ),
            ),
        )
        api = toolkit_client.tool.transformations.external_data_sources
        try:
            upserted = api.upsert([source])
            assert len(upserted) == 1
            assert upserted[0].external_id == external_id

            listed = api.list(limit=None)
            assert external_id in {item.external_id for item in listed}

            usability = api.verify_usability(external_id)
            assert usability.usable_version is not None
        finally:
            api.delete([source.as_id()])
