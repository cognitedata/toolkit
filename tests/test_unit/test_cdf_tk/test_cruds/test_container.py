from pathlib import Path
from unittest.mock import MagicMock

import pytest
from cognite.client.data_classes.data_modeling import Container, ContainerProperty, Text

from cognite_toolkit._cdf_tk._parameters import read_parameters_from_dict
from cognite_toolkit._cdf_tk.cruds import ContainerCRUD, ResourceWorker
from tests.test_unit.approval_client import ApprovalToolkitClient


class TestContainerLoader:
    @pytest.mark.parametrize(
        "item",
        [
            pytest.param(
                {
                    "properties": {
                        "myDirectRelation": {
                            "name": "my direct relation",
                            "type": {
                                "type": "direct",
                                "container": {
                                    "type": "container",
                                    "space": "sp_my_space",
                                    "externalId": "my_container",
                                },
                            },
                        }
                    }
                },
                id="Direct relation property with require constraint.",
            ),
        ],
    )
    def test_valid_spec(self, item: dict):
        spec = ContainerCRUD.get_write_cls_parameter_spec()
        dumped = read_parameters_from_dict(item)

        extra = dumped - spec

        assert not extra, f"Extra keys: {extra}"

    def test_unchanged_used_for_not_set(self, toolkit_client_approval: ApprovalToolkitClient) -> None:
        loader = ContainerCRUD.create_loader(toolkit_client_approval.mock_client)
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
        cdf_container = Container(
            space="sp_enterprise_process_industry_full",
            external_id="Toolkit360Image",
            last_updated_time=1739469813633,
            created_time=1739469813633,
            description=None,
            name=None,
            used_for="node",
            is_global=False,
            properties={"UUID": ContainerProperty(type=Text())},
            indexes={},
            constraints={},
        )

        toolkit_client_approval.append(Container, [cdf_container])

        worker = ResourceWorker(loader, "deploy")
        resources = worker.prepare_resources([file])
        assert {
            "create": len(resources.to_create),
            "change": len(resources.to_update),
            "delete": len(resources.to_delete),
            "unchanged": len(resources.unchanged),
        } == {"create": 0, "change": 0, "delete": 0, "unchanged": 1}
