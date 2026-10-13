from collections import defaultdict
from collections.abc import Iterable

from cognite_toolkit._cdf_tk.client._resource_base import (
    Identifier,
)
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource, contains_unresolved_variable
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
)
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO

from ._base import (
    InternalValidatorException,
    RuleSetStatus,
    ToolkitGlobalRuleSet,
    identifier_values,
    quote_identifier,
    with_position,
)


class DependencyRuleSet(ToolkitGlobalRuleSet):
    """
    Validates that resources reference each other correctly, and that local state changes
    can actually be applied in CDF.
    """

    DISPLAY_NAME = "Dependencies"

    @staticmethod
    def _is_unresolved(identifier: Identifier) -> bool:
        """Unresolved variables are reported on their own, and always lead to unknown references."""
        return contains_unresolved_variable(identifier)

    def get_status(self) -> RuleSetStatus:
        if self.client is None:
            return RuleSetStatus(
                code="reduced",
                message="No client provided, will only validate dependencies between resources within the provided modules, but not validate against CDF.",
            )
        else:
            return RuleSetStatus(code="ready", message="Will validate dependencies and state changes against CDF.")

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        """CDF dependency validations are validations that require checking the existence of resources in CDF."""
        built_resource_ids: set[tuple[type[ResourceIO], Identifier]] = {
            (resource.crud_cls, resource.identifier) for module in self.modules for resource in module.resources
        }
        missing_locally_by_crud_cls: dict[type[ResourceIO], dict[Identifier, list[BuiltResource]]] = defaultdict(
            lambda: defaultdict(list)
        )
        for module in self.modules:
            for resource in module.resources:
                for crud_cls, dependency_id in resource.dependencies:
                    if (crud_cls, dependency_id) not in built_resource_ids and not self._is_unresolved(dependency_id):
                        missing_locally_by_crud_cls[crud_cls][dependency_id].append(resource)

        if self.client:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                crud = crud_cls(self.client)
                resource_label = crud_cls.kind.lower()
                try:
                    existing_in_cdf = {
                        crud.get_id(cdf_item) for cdf_item in crud.retrieve(list(expected_by_identifier.keys()))
                    }
                except ToolkitAPIError as e:
                    yield InternalValidatorException(
                        message=f"Failed to verify existence of {resource_label} in CDF: {e}",
                        source=crud.kind,
                    )
                    continue
                if missing := set(expected_by_identifier.keys()) - existing_in_cdf:
                    for identifier in missing:
                        for resource in expected_by_identifier[identifier]:
                            yield with_position(
                                BuildError(
                                    code="MISSING-REFERENCED-RESOURCE",
                                    message=f"The {resource_label} {quote_identifier(identifier)} does not exist locally or in CDF. "
                                    f"It is referenced by {quote_identifier(resource.identifier)}.",
                                    fix=f"Ensure that the {resource_label} exists or remove the reference to it.",
                                    source_file=resource.source_path,
                                ),
                                values=identifier_values(identifier),
                                variables=resource.variables,
                            )
        else:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                resource_type_name = f"{crud_cls.kind.lower()} ({crud_cls.folder_name})"
                for identifier, expected_resources in expected_by_identifier.items():
                    for resource in expected_resources:
                        yield with_position(
                            BuildWarning(
                                code="UNVERIFIED-REFERENCED-RESOURCE",
                                message=f"Missing {resource_type_name} {quote_identifier(identifier)}. "
                                f"It is referenced by {quote_identifier(resource.identifier)}.",
                                fix=f"Provide credentials to enable CDF verification. "
                                f"Or ensure that {resource_type_name} exists or remove the reference to it.",
                                source_file=resource.source_path,
                            ),
                            values=identifier_values(identifier),
                            variables=resource.variables,
                        )
