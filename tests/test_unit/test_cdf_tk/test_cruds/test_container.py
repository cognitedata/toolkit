from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.client.identifiers import ContainerId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    ContainerPropertyDefinition,
    ContainerRequest,
    ContainerResponse,
    DirectNodeRelation,
    RequiresConstraintDefinition,
    TextProperty,
)
from cognite_toolkit._cdf_tk.client.testing import monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.constants import CONTAINER_UPSERT_BATCH_LIMIT
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


class TestContainerDeployTopologicalSort:
    def test_requires_constraint_dependency_ordering(self) -> None:
        dependency_container = ContainerRequest(
            space="sp_space",
            external_id="Dependency",
            properties={"name": ContainerPropertyDefinition(type=TextProperty())},
        )
        dependent_container = ContainerRequest(
            space="sp_space",
            external_id="Dependent",
            properties={"name": ContainerPropertyDefinition(type=TextProperty())},
            constraints={
                "requiresDependency": RequiresConstraintDefinition(
                    require=ContainerId(space="sp_space", external_id="Dependency")
                )
            },
        )

        with monkeypatch_toolkit_client() as client:
            loader = ContainerCRUD(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches([dependent_container, dependency_container])

        flat_ids = [container.external_id for batch in batches for container in batch]
        assert flat_ids.index("Dependency") < flat_ids.index("Dependent")

    def test_direct_relation_dependency_ordering(self) -> None:
        dependency_container = ContainerRequest(
            space="sp_space",
            external_id="Dependency",
            properties={"name": ContainerPropertyDefinition(type=TextProperty())},
        )
        dependent_container = ContainerRequest(
            space="sp_space",
            external_id="Dependent",
            properties={
                "ref": ContainerPropertyDefinition(
                    type=DirectNodeRelation(container=ContainerId(space="sp_space", external_id="Dependency"))
                )
            },
        )

        with monkeypatch_toolkit_client() as client:
            loader = ContainerCRUD(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches([dependent_container, dependency_container])

        flat_ids = [container.external_id for batch in batches for container in batch]
        assert flat_ids.index("Dependency") < flat_ids.index("Dependent")

    def test_many_dependents_split_across_batches_after_dependency(self) -> None:
        # Reproduces the reported bug: a single dependency-free container, and more containers
        # requiring it than fit in a single upsert batch.
        container_count = CONTAINER_UPSERT_BATCH_LIMIT + 25
        dependency_container = ContainerRequest(
            space="sp_space",
            external_id="Dependency",
            properties={"name": ContainerPropertyDefinition(type=TextProperty())},
        )
        dependents = [
            ContainerRequest(
                space="sp_space",
                external_id=f"Dependent_{i}",
                properties={"name": ContainerPropertyDefinition(type=TextProperty())},
                constraints={
                    "requiresDependency": RequiresConstraintDefinition(
                        require=ContainerId(space="sp_space", external_id="Dependency")
                    )
                },
            )
            for i in range(container_count)
        ]

        with monkeypatch_toolkit_client() as client:
            loader = ContainerCRUD(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches([*dependents, dependency_container])

        assert len(batches) > 1, "Should split into multiple batches given the batch limit"
        assert batches[0][0].external_id == "Dependency", "Dependency-free container must be sent first"
        flat_ids = [container.external_id for batch in batches for container in batch]
        for dependent in dependents:
            assert flat_ids.index("Dependency") < flat_ids.index(dependent.external_id)

    def test_large_scc_kept_in_single_batch(self) -> None:
        container_count = CONTAINER_UPSERT_BATCH_LIMIT + 25
        containers = [
            ContainerRequest(
                space="sp_space",
                external_id=f"Container_{i}",
                properties={"name": ContainerPropertyDefinition(type=TextProperty())},
                constraints={
                    "requiresNext": RequiresConstraintDefinition(
                        # Cyclic dependency across all containers
                        require=ContainerId(space="sp_space", external_id=f"Container_{(i + 1) % container_count}")
                    )
                },
            )
            for i in range(container_count)
        ]

        with monkeypatch_toolkit_client() as client:
            loader = ContainerCRUD(client, Path("build_dir"), None)
            batches = loader._compute_deploy_batches(containers)

        assert len(batches) == 1, "All containers in one SCC should stay in a single batch"
        assert len(batches[0]) == container_count
