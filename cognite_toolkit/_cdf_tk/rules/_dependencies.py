from collections import defaultdict
from collections.abc import Iterable

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import ConsistencyError, Insight
from cognite_toolkit._cdf_tk.cruds import ResourceCRUD

from ._base import FailedValidation, RuleSetStatus, ToolkitGlobalRulSet


class DependencyRuleSet(ToolkitGlobalRulSet):
    CODE_PREFIX = "MISSING-DEPENDENCY"
    DISPLAY_NAME = "dependencies"

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            message = "No client provided, will only validate dependencies between resources within the provided modules, but not validate against CDF."
        else:
            message = "Will validate dependencies against CDF."
        return RuleSetStatus(code="ready", message=message)

    def validate(self) -> Iterable[Insight | FailedValidation]:
        """CDF dependency validations are validations that require checking the existence of resources in CDF."""
        built_resource_ids: set[tuple[type[ResourceCRUD], Identifier]] = {
            (resource.crud_cls, resource.identifier) for module in self.modules for resource in module.resources
        }
        missing_locally_by_crud_cls: dict[type[ResourceCRUD], dict[Identifier, list[BuiltResource]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for module in self.modules:
            for resource in module.resources:
                for crud_cls, dependency_id in resource.dependencies:
                    if (crud_cls, dependency_id) not in built_resource_ids:
                        missing_locally_by_crud_cls[crud_cls][dependency_id].append(resource)

        if self.client:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                crud = crud_cls(self.client, None, None)
                display_name = crud.display_name
                existing_in_cdf = {
                    crud.get_id(cdf_item) for cdf_item in crud.retrieve(list(expected_by_identifier.keys()))
                }
                if missing := set(expected_by_identifier.keys()) - existing_in_cdf:
                    for identifier in missing:
                        expected_resources = expected_by_identifier[identifier]
                        referenced_str = self._create_reference_string(expected_resources)
                        yield ConsistencyError(
                            code=f"{self.CODE_PREFIX}-CDF",
                            message=f"{identifier} {display_name} does not exist locally or in CDF. It is referenced by: \n{referenced_str}",
                            fix=f"Ensure that {display_name} exists or removed the reference to it.",
                        )
        else:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                resource_type_name = f"{crud_cls.kind.lower()} ({crud_cls.folder_name})"
                for identifier, expected_resources in expected_by_identifier.items():
                    referenced_str = self._create_reference_string(expected_resources)
                    yield ConsistencyError(
                        code=f"{self.CODE_PREFIX}-LOCAL",
                        message=f"{identifier} {resource_type_name} does not exist. It is referenced by: \n{referenced_str}",
                        fix=f"If the {resource_type_name} exist in CDF, provide client credentials to not get this error. "
                        f"Or ensure that {resource_type_name} exists or removed the reference to it.",
                    )

    def _create_reference_string(self, expected_resources: list[BuiltResource]) -> str:
        return " - ".join(
            f"{resource.identifier!s} in {resource.source_path.as_posix()!r}" for resource in expected_resources
        )
