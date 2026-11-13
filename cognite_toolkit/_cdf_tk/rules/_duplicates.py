from collections import defaultdict
from collections.abc import Iterable
from pathlib import Path

from cognite_toolkit._cdf_tk.client._resource_base import Identifier
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import contains_unresolved_variable
from cognite_toolkit._cdf_tk.data_classes._insights import (
    BuildWarning,
    Insight,
    InternalValidatorException,
)
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.resource_ios import ResourceType
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.file import relative_to_modules

from ._base import (
    ResourceRef,
    RuleSetStatus,
    ToolkitRuleSet,
    identifier_values,
    quote_identifier,
    with_position,
)


class DuplicateRuleSet(ToolkitRuleSet):
    """Warns about resources of the same type defined with the same identifier.

    Only the first definition is deployed, the others are skipped.
    """

    RESOURCE_TYPES = frozenset({"ALL"})
    CONTEXT = frozenset({"local"})
    DISPLAY_NAME = "Duplicates"

    def get_status(self) -> RuleSetStatus:
        if not Flags.ALPHA_RULES.is_enabled():
            return RuleSetStatus(
                code="skip",
                message="Duplicate identifier validation is an alpha rule. Enable the 'alpha-rules' alpha flag to run it.",
            )
        return RuleSetStatus(code="ready", message="Will check for resources with duplicated identifiers.")

    def validate(self) -> Iterable[Insight | InternalValidatorException]:
        if not Flags.ALPHA_RULES.is_enabled():
            return
        definitions_by_id: dict[tuple[ResourceType, Identifier], list[tuple[ResourceRef, Path]]] = defaultdict(list)
        for resource in self.resources:
            # Unresolved variables are reported on their own.
            if isinstance(resource.source, Path) and not contains_unresolved_variable(resource.identifier):
                definitions_by_id[(resource.type, resource.identifier)].append((resource, resource.source))

        for (resource_type, identifier), definitions in definitions_by_id.items():
            if len(definitions) < 2:
                continue
            display_paths = [relative_to_modules(source) for _, source in definitions]
            for index, (resource, source) in enumerate(definitions[1:], start=1):
                yield self._create_warning(resource_type, resource, source, display_paths, index)

    @staticmethod
    def _create_warning(
        resource_type: ResourceType, resource: ResourceRef, source: Path, display_paths: list[str], index: int
    ) -> BuildWarning:
        message = (
            f"The {resource_type.kind.lower()} {quote_identifier(resource.identifier)} is already defined in "
            f"{display_paths[0]}, which will be deployed instead."
        )
        if others := list(dict.fromkeys(display_paths[1:index] + display_paths[index + 1 :])):
            message += f" It is also defined in {humanize_collection(others, sort=False)}."
        warning = BuildWarning(
            code="DUPLICATED-IDENTIFIER",
            message=message,
            fix="Remove the duplicate definition or give it a unique identifier.",
            source_file=source,
            alpha=True,
        )
        return with_position(warning, values=identifier_values(resource.identifier), variables=resource.variables)
