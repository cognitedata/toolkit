from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerResponse,
    TextProperty,
)
from cognite_toolkit._cdf_tk.resource_ios import ContainerCRUD, ResourceWorker
from tests.test_unit.approval_client import ApprovalToolkitClient


@pytest.fixture
def cdf_container() -> ContainerResponse:
    return ContainerResponse(
        space="sp_enterprise_process_industry_full",
        external_id="Toolkit360Image",
        last_updated_time=1739469813633,
        created_time=1739469813633,
        description=None,
        name=None,
        used_for="node",
        is_global=False,
        properties={
            "UUID": ContainerPropertyDefinition(
                type=TextProperty(list=False, collation="ucs_basic"),
                immutable=False,
                nullable=True,
                auto_increment=False,
            )
        },
        indexes={},
        constraints={},
    )


class TestContainerCRUD:
    def test_unchanged_used_for_not_set(
        self, toolkit_client_approval: ApprovalToolkitClient, cdf_container: ContainerResponse
    ) -> None:
        crud = ContainerCRUD.create_loader(toolkit_client_approval.mock_client)
        raw_file = """space: sp_enterprise_process_industry_full
externalId: Toolkit360Image
properties:
  UUID:
    type:
      list: false
      collation: ucs_basic
      type: text
    immutable: false
    nullable: true
    autoIncrement: false
constraints: {}
indexes: {}
"""
        file = MagicMock(spec=Path)
        file.read_text.return_value = raw_file

        toolkit_client_approval.append(ContainerResponse, [cdf_container])

        worker = ResourceWorker(crud, "deploy")
        resources = worker.prepare_resources([file])
        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}

        dumped_no_local = crud.dump_resource(cdf_container)
        assert "usedFor" in dumped_no_local

    def test_only_in_cdf_properties_listed(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        crud = ContainerCRUD.create_loader(toolkit_client_approval.mock_client)
        item_id = ContainerId(space="my_space", external_id="MyContainer")

        local_dict = {"properties": {"name": {"type": {"type": "text"}}}}
        cdf_dict = {
            "properties": {
                "name": {"type": {"type": "text"}},
                "attempted_deleted_field": {"type": {"type": "int32"}},
            }
        }

        with patch(
            "cognite_toolkit._cdf_tk.resource_ios._resource_ios.datamodel.HighSeverityWarning"
        ) as mock_warning_cls:
            mock_warning_cls.return_value.print_warning = MagicMock()
            crud._print_container_diff_warning(item_id, local_dict, cdf_dict)

        message = mock_warning_cls.call_args[0][0]
        assert "attempted_deleted_field" in message

    def test_dump_resource_normalizes_empty_constraints_and_indexes_to_local_shape(
        self, toolkit_client_approval: ApprovalToolkitClient, cdf_container: ContainerResponse
    ) -> None:
        crud = ContainerCRUD.create_loader(toolkit_client_approval.mock_client)

        local_with_null = {"constraints": None, "indexes": None}
        dumped = crud.dump_resource(cdf_container, local_with_null)
        assert dumped.get("constraints") is None
        assert dumped.get("indexes") is None

        local_with_empty_dict = {"constraints": {}, "indexes": {}}
        dumped = crud.dump_resource(cdf_container, local_with_empty_dict)
        assert dumped.get("constraints") == {}
        assert dumped.get("indexes") == {}

        local_absent: dict = {}
        dumped = crud.dump_resource(cdf_container, local_absent)
        assert "constraints" not in dumped
        assert "indexes" not in dumped
