from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

from cognite_toolkit._cdf_tk.client._resource_base import (
    Identifier,
)
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import contains_unresolved_variable
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildError,
    BuildWarning,
    Insight,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO, ResourceType
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import relative_to_modules

from ._base import (
    InternalValidatorException,
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
    identifier_values,
    quote_identifier,
    with_position,
)


class DependencyRuleSet(ToolkitRuleSet):
    """
    Validates that resources reference each other correctly, and that local state changes
    can actually be applied in CDF.
    """

    RESOURCE_TYPES = frozenset({"ALL"})
    CONTEXT = frozenset({"local"})

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
        if Flags.ALPHA_RULES.is_enabled():
            yield from self._validate_duplicates()
        yield from self._validate_references()

    def _validate_duplicates(self) -> Iterable[Insight]:
        """Resources with the same identifier overwrite each other, only the first definition is deployed."""
        resources_by_id: dict[tuple[ResourceType, Identifier], list[tuple[ResourceRef, Path]]] = defaultdict(list)
        for resource in self.resources:
            if isinstance(resource.source, Path) and not self._is_unresolved(resource.identifier):
                resources_by_id[(resource.type, resource.identifier)].append((resource, resource.source))

        for (resource_type, identifier), definitions in resources_by_id.items():
            if len(definitions) < 2:
                continue
            display_paths = [relative_to_modules(source_path) for _, source_path in definitions]
            for index, (resource, source_path) in enumerate(definitions[1:], start=1):
                other_paths = list(dict.fromkeys(display_paths[:index] + display_paths[index + 1 :]))
                yield with_position(
                    BuildWarning(
                        code="DUPLICATED-IDENTIFIER",
                        message=f"The {resource_type.kind.lower()} {quote_identifier(identifier)} is also defined in "
                        f"{humanize_collection(other_paths, sort=False)}. "
                        f"Only the definition in {display_paths[0]} will be deployed.",
                        fix="Remove the duplicate definition or give it a unique identifier.",
                        source_file=source_path,
                        alpha=True,
                    ),
                    values=identifier_values(identifier),
                    variables=resource.variables,
                )

    def _validate_references(self) -> Iterable[Insight | InternalValidatorException]:
        """CDF dependency validations are validations that require checking the existence of resources in CDF."""
        local_resource_ids: set[tuple[ResourceType, Identifier]] = {
            (resource.type, resource.identifier) for resource in self.resources
        }
        missing_locally_by_crud_cls: dict[type[ResourceIO], dict[Identifier, list[tuple[ResourceRef, Path]]]] = (
            defaultdict(lambda: defaultdict(list))
        )
        for resource in self.resources:
            source_path = resource.source
            if not isinstance(source_path, Path):
                continue
            for crud_cls, dependency_id in resource.dependencies:
                if (
                    crud_cls.as_resource_type(),
                    dependency_id,
                ) not in local_resource_ids and not self._is_unresolved(dependency_id):
                    missing_locally_by_crud_cls[crud_cls][dependency_id].append((resource, source_path))

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
                        for resource, source_path in expected_by_identifier[identifier]:
                            yield with_position(
                                BuildError(
                                    code="MISSING-REFERENCED-RESOURCE",
                                    message=f"The {resource_label} {quote_identifier(identifier)} does not exist locally or in CDF. "
                                    f"It is referenced by {quote_identifier(resource.identifier)}.",
                                    fix=f"Ensure that the {resource_label} exists or remove the reference to it.",
                                    source_file=source_path,
                                ),
                                values=identifier_values(identifier),
                                variables=resource.variables,
                            )
        else:
            for crud_cls, expected_by_identifier in missing_locally_by_crud_cls.items():
                resource_type_name = f"{crud_cls.kind.lower()} ({crud_cls.folder_name})"
                for identifier, expected_resources in expected_by_identifier.items():
                    for resource, source_path in expected_resources:
                        yield with_position(
                            BuildWarning(
                                code="UNVERIFIED-REFERENCED-RESOURCE",
                                message=f"Missing {resource_type_name} {quote_identifier(identifier)}. "
                                f"It is referenced by {quote_identifier(resource.identifier)}.",
                                fix=f"Provide credentials to enable CDF verification. "
                                f"Or ensure that {resource_type_name} exists or remove the reference to it.",
                                source_file=source_path,
                            ),
                            values=identifier_values(identifier),
                            variables=resource.variables,
                        )
