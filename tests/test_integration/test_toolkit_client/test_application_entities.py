import pytest
from cognite.client.exceptions import CogniteAPIError

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.data_classes.application_entities import (
    ApplicationEntity,
    ApplicationEntityWrite,
)
from cognite_toolkit._cdf_tk.constants import APPLICATION_NAME
from tests.test_integration.constants import RUN_UNIQUE_ID

ENTITY_SET = dict(data_namespace=APPLICATION_NAME, entity_set="test")


class TestApplicationEntities:
    def test_create_retrieve_delete(self, toolkit_client: ToolkitClient) -> None:
        entity = ApplicationEntityWrite(
            external_id=f"test_create_retrieve_delete_{RUN_UNIQUE_ID}",
            visibility="private",
            data={"key": "value"},
        )
        try:
            created = toolkit_client.application_entities.create(entity, **ENTITY_SET)
            assert isinstance(created, ApplicationEntity)
            assert created.as_write().dump() == entity.dump()

            retrieved = toolkit_client.application_entities.retrieve(entity.external_id, **ENTITY_SET)

            assert isinstance(retrieved, ApplicationEntity)
            assert retrieved.as_write().dump() == entity.dump()
        finally:
            toolkit_client.application_entities.delete(entity.external_id, **ENTITY_SET)

        with pytest.raises(CogniteAPIError):
            toolkit_client.application_entities.retrieve(entity.external_id, **ENTITY_SET)
