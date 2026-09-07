import json
import re
import tempfile
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml
from rich.console import Console, Group, RenderableType

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier, ResponseResource
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import (
    BuildFolder,
    BuildParameters,
    BuildVariable,
    BuiltResource,
    ResourceType,
)
from cognite_toolkit._cdf_tk.constants import ENV_VAR_PATTERN, HINT_LEAD_TEXT
from cognite_toolkit._cdf_tk.data_classes import YAMLComments
from cognite_toolkit._cdf_tk.exceptions import ToolkitError, ToolkitMissingResourceError, ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios import (
    ExtractionPipelineConfigIO,
    ResourceIO,
    ViewIO,
)
from cognite_toolkit._cdf_tk.ui import (
    ToolkitPanel,
    ToolkitPanelSection,
    ToolkitTable,
    hanging_indent,
)
from cognite_toolkit._cdf_tk.utils import (
    humanize_collection,
    read_yaml_content,
    safe_read,
)
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree, yaml_safe_dump

from ._base import ToolkitCommand

_VARIABLE_PATTERN = re.compile(r"\{\{(.+?)\}\}")
# The encoding and newline characters to use when writing files
# These are hardcoded to ensure that running the pull command on different platforms
# will produce the same output. The motivation is when having local sources in
# version control, the diff will be easier to read.
ENCODING = "utf-8"
NEWLINE = "\n"


@dataclass
class SkippedPull:
    identifier: Identifier
    reason: str


@dataclass
class PullResult:
    """Represents the result of a pull operation for a single resource file.

    Attributes:
        source_file: The path to the source file that was pulled.
        has_changes: A boolean indicating whether there were changes between the local and CDF versions.
        is_dry_run: A boolean indicating whether the pull was a dry run (no files were modified).
        resource_type: The type of resource that was pulled (e.g., "view", "extraction_pipeline_config").
    """

    source_file: Path
    resource_type: ResourceType
    has_changes: bool
    is_dry_run: bool
    extra_files: list[Path]
    skipped: list[SkippedPull]


class PullV2Command(ToolkitCommand):
    def pull(
        self,
        user_selected_modules: list[str] | None,
        env_vars: EnvironmentVariables,
        organization_dir: Path,
        config_yaml: Path | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Pulls resources from CDF and updates local configuration files.

        Args:
            user_selected_modules: List of module names or paths to pull. If None, the user will be prompted to select.
            env_vars: Environment variables for the current environment.
            organization_dir: Path to the organization directory containing modules.
            config_yaml: Optional path to a specific configuration YAML file to pull.
            dry_run: If True, no files will be modified; only a summary of changes will be displayed.
            verbose: If True, detailed output will be printed during execution.
        """
        client = env_vars.get_client(is_strict_validation=False)
        console = client.console
        build_dir = Path(tempfile.mkdtemp())
        try:
            parameters = BuildParameters(
                organization_dir=organization_dir,
                build_dir=build_dir,
                config_yaml=config_yaml,
                user_selected_modules=user_selected_modules,
                verbose=False,
                write_insights=False,
                write_lineage=False,
            )
            build_folder = BuildV2Command(print_warning=False, skip_tracking=True, silent=True, client=client).build(
                parameters, client, display=False
            )
        except ToolkitError as e:
            raise ToolkitError(f"Failed to build module {humanize_collection(user_selected_modules or '')}.") from e
        else:
            self._pull_built_modules(build_folder, client, dry_run, env_vars.dump(include_os=True), console, verbose)
        finally:
            try:
                safe_rmtree(build_dir)
            except Exception as e:
                raise ToolkitError(f"Failed to clean up temporary build directory {build_dir}.") from e

    def _pull_built_modules(
        self,
        build_folder: BuildFolder,
        client: ToolkitClient,
        dry_run: bool,
        env_vars: dict[str, str | None],
        console: Console,
        verbose: bool,
    ) -> list[PullResult]:
        resources_by_type: dict[ResourceType, list[BuiltResource]] = defaultdict(list)
        for module in build_folder.built_modules:
            for resource in module.resources:
                resources_by_type[resource.type].append(resource)

        results: list[PullResult] = []
        for resource_type, resources in resources_by_type.items():
            resource_io = resources[0].crud_cls.create_loader(
                client, build_dir=build_folder.build_dir, console=client.console
            )

            cdf_resources = resource_io.retrieve([resource.identifier for resource in resources])
            cdf_resource_by_id = {resource_io.get_id(r): r for r in cdf_resources}

            resource_by_source_file: dict[Path, list[BuiltResource]] = defaultdict(list)
            for resource in resources:
                resource_by_source_file[resource.source_path].append(resource)

            for source_file, file_resources in resource_by_source_file.items():
                local_resource_by_id = self._get_local_resource_dict_by_id(file_resources, resource_io, env_vars)
                has_changes, missing_in_cdf, to_write = self._get_to_write(
                    local_resource_by_id, cdf_resource_by_id, resource_io
                )

                extra_files: dict[Path, str] = {}
                if has_changes and not dry_run:
                    new_content, extra_files = self._to_write_content(
                        safe_read(source_file), to_write, file_resources, env_vars, resource_io, source_file
                    )
                    with source_file.open("w", encoding=ENCODING, newline=NEWLINE) as f:
                        f.write(new_content)
                    for filepath, content in extra_files.items():
                        filepath.parent.mkdir(parents=True, exist_ok=True)
                        with filepath.open("w", encoding=ENCODING, newline=NEWLINE) as f:
                            f.write(content)

                results.append(
                    PullResult(
                        source_file=source_file,
                        has_changes=has_changes,
                        is_dry_run=dry_run,
                        resource_type=resource_type,
                        extra_files=list(extra_files.keys()),
                        skipped=[
                            SkippedPull(identifier=identifier, reason="Resource missing in CDF")
                            for identifier in missing_in_cdf
                        ],
                    )
                )

        self._display_results(results, console, verbose)

        return results

    @classmethod
    def _display_results(cls, results: list[PullResult], console: Console, verbose: bool) -> None:
        if not results:
            console.print(
                ToolkitPanel(
                    "No resources were pulled.",
                    title="Pull summary",
                )
            )
            return

        is_dry_run = any(r.is_dry_run for r in results)
        panel_title = "Pull summary"
        if is_dry_run:
            panel_title += " [dim](dry run)[/]"

        # Group results by resource type
        results_by_type: dict[ResourceType, list[PullResult]] = defaultdict(list)
        for result in results:
            results_by_type[result.resource_type].append(result)

        table = ToolkitTable()
        table.add_column("Resource", style="cyan")
        if is_dry_run:
            table.add_column("Would change", justify="right", style="yellow")
        else:
            table.add_column("Changed", justify="right", style="yellow")
        table.add_column("Unchanged", justify="right", style="dim")
        table.add_column("Skipped", justify="right", style="yellow")
        table.add_column("Extra files", justify="right", style="green")
        table.add_column("Total", justify="right", style="cyan")

        total_changed = 0
        total_unchanged = 0
        total_skipped = 0
        total_extra = 0
        total_files = 0
        all_skipped: list[tuple[ResourceType, Path, SkippedPull]] = []

        # Sort types for stable output
        sorted_types = sorted(results_by_type.keys(), key=lambda t: (t.resource_folder, t.kind))
        for resource_type in sorted_types:
            type_results = results_by_type[resource_type]
            changed = sum(1 for r in type_results if r.has_changes)
            unchanged = sum(1 for r in type_results if not r.has_changes)
            skipped = sum(len(r.skipped) for r in type_results)
            extra = sum(len(r.extra_files) for r in type_results)
            total = len(type_results)

            total_changed += changed
            total_unchanged += unchanged
            total_skipped += skipped
            total_extra += extra
            total_files += total

            for r in type_results:
                for skip in r.skipped:
                    all_skipped.append((resource_type, r.source_file, skip))

            table.add_row(
                str(resource_type),
                str(changed),
                str(unchanged),
                str(skipped),
                str(extra),
                str(total),
            )

        if len(sorted_types) > 1:
            table.add_section()
            table.add_row(
                "[bold]All[/]",
                f"[bold]{total_changed}[/]",
                f"[bold]{total_unchanged}[/]",
                f"[bold]{total_skipped}[/]",
                f"[bold]{total_extra}[/]",
                f"[bold]{total_files}[/]",
            )

        sections: list[RenderableType] = [ToolkitPanelSection(content=[table.as_panel_detail()])]

        if is_dry_run:
            sections.append(
                ToolkitPanelSection(
                    description=(
                        f"{HINT_LEAD_TEXT}This was a dry run. No files were modified. "
                        f"Re-run without --dry-run to apply the changes."
                    )
                )
            )

        if all_skipped and not verbose:
            most_common = Counter(skip.reason for _, _, skip in all_skipped).most_common(n=3)
            sections.append(
                ToolkitPanelSection(
                    description=(
                        f"{HINT_LEAD_TEXT}A total of {len(all_skipped)} resources were skipped during pull. "
                        f"The most common reasons were: "
                        f"{', '.join(f'{reason} ({count} occurrences)' for reason, count in most_common)}. "
                        f"Use --verbose to see all skipped resources."
                    )
                )
            )
        elif verbose and all_skipped:
            sections.append(
                ToolkitPanelSection(
                    title="Skipped resources",
                    content=[
                        hanging_indent(
                            "○",
                            f"[bold]{skip.identifier}[/] {source_file.as_posix()} [{resource_type}] {skip.reason}",
                            marker_style="dim",
                        )
                        for resource_type, source_file, skip in all_skipped
                    ],
                )
            )

        if verbose:
            changed_entries: list[RenderableType] = []
            for resource_type in sorted_types:
                for r in results_by_type[resource_type]:
                    if not r.has_changes:
                        continue
                    changed_entries.append(
                        hanging_indent(
                            "●",
                            f"[bold]{resource_type}[/] {r.source_file.as_posix()}"
                            + (
                                f" (+{len(r.extra_files)} extra file{'s' if len(r.extra_files) != 1 else ''})"
                                if r.extra_files
                                else ""
                            ),
                            marker_style="green",
                        )
                    )
            if changed_entries:
                sections.append(
                    ToolkitPanelSection(
                        title="Changed files",
                        content=changed_entries,
                    )
                )

        console.print(ToolkitPanel(Group(*sections), title=panel_title))

    @staticmethod
    def _get_local_resource_dict_by_id(
        resources: list[BuiltResource],
        resource_io: ResourceIO,
        environment_variables: dict[str, str | None],
    ) -> dict[Identifier, dict[str, Any]]:
        local_resource_by_id: dict[Identifier, dict[str, Any]] = {}
        for resource in resources:
            resource_list = resource_io.load_resource_file(resource.build_path, environment_variables)
            # In build, there should always be just one resource per file, so we can safely take the first one.
            # Adding a try-except block to catch any unexpected IndexError, which would indicate a bug in the Toolkit.
            try:
                local_resource_by_id[resource.identifier] = resource_list[0]
            except IndexError:
                raise ToolkitValueError(
                    f"There is a bug in Toolkit. "
                    f"Expected at least one resource in the file {resource.build_path}, but found none."
                )
        return local_resource_by_id

    @staticmethod
    def _get_to_write(
        local_resource_by_id: dict[Identifier, dict[str, Any]],
        cdf_resource_by_id: dict[Identifier, ResponseResource],
        resource_io: ResourceIO,
    ) -> tuple[bool, list[Identifier], dict[Identifier, dict[str, Any]]]:
        to_write: dict[Identifier, dict[str, Any]] = {}
        has_changes = False
        missing_in_cdf: list[Identifier] = []
        for item_id, local_dict in local_resource_by_id.items():
            cdf_resource = cdf_resource_by_id.get(item_id)
            if cdf_resource is None:
                to_write[item_id] = local_dict
                missing_in_cdf.append(item_id)
                continue
            cdf_dumped = resource_io.dump_resource(cdf_resource, local_dict)

            if cdf_dumped == local_dict:
                to_write[item_id] = local_dict
            else:
                to_write[item_id] = cdf_dumped
                has_changes = True
        return has_changes, missing_in_cdf, to_write

    def _to_write_content(
        self,
        source_content: str,
        to_write: dict[Identifier, dict[str, Any]],
        resources: list[BuiltResource],
        environment_variables: dict[str, str | None],
        resource_io: ResourceIO,
        source_file: Path,
    ) -> tuple[str, dict[Path, str]]:
        """Convert resource data from CDF into YAML file content ready to be written to disk.

        This method takes the raw CDF resource data and transforms it back into a properly
        formatted YAML file that preserves:
        - Template variables (e.g., {{ variable_name }}) instead of their resolved values
        - YAML comments from the original source file
        - The original key ordering in dictionaries

        The transformation process:
        1. Replace all template variables with unique placeholders
        2. Load source YAML content while preserving comments
        3. Update the resource data with placeholder values where variables were used
        4. Dump the updated data back to YAML format
        5. Replace placeholders with the original template variable syntax
        6. Restore the YAML comments

        Args:
            source: The original YAML file content as a string.
            to_write: A mapping from resource identifiers to their updated data dictionaries
                pulled from CDF.
            resources: The list of built resources containing build variables and metadata.
            environment_variables: A mapping of environment variable names to their values,
                used to resolve variables like ${VAR_NAME} in template values.
            resource_io: The ResourceCRUD loader instance for this resource type.
            source_file: The path to the source file being processed.

        Returns:
            A tuple containing:
            - The final YAML content string ready to be written to disk.
            - A dictionary mapping extra file paths to their content (for resources
              that have additional files, like SQL queries for transformations).

        Raises:
            ValueError: If the loaded YAML structure doesn't match between the original
                and placeholder versions.
            ToolkitMissingResourceError: If a resource identifier is not found in the
                to_write or resources mappings.
        """
        # 1. Replace all variables with placeholders
        # 2. Load source and keep the comments
        # 3. Update the to_write dict with the placeholders
        # 4. Dump the yaml with the placeholders
        # 5. Replace the placeholders with the variables
        # 6. Add the comments back

        # All resources are assumed to be in the same file, and thus the same build variables.
        variables = resources[0].variables
        if environment_variables:
            variables = self._resolve_env_vars_in_variables(variables, environment_variables)

        content, value_by_placeholder = BuildVariable.substitute_with_placeholders(source_content, variables)
        comments = YAMLComments.load(source_content)

        # If there is a variable in the identifier, we need to replace it with the value
        # such that we can look it up in the to_write dict.

        if isinstance(resource_io, ExtractionPipelineConfigIO):
            # The safe read in ExtractionPipelineConfigLoader stringifies the config dict,
            # but we need to load it as a dict so we can write it back to the file maintaining
            # the order or the keys.
            source_dict_with_variable_substitution = read_yaml_content(
                BuildVariable.substitute(source_content, variables, source_file.suffix)
            )
            source_with_with_variable_placeholders = read_yaml_content(content)
        else:
            source_dict_with_variable_substitution = read_yaml_content(
                resource_io.safe_read(BuildVariable.substitute(source_content, variables, source_file.suffix))
            )
            source_with_with_variable_placeholders = read_yaml_content(resource_io.safe_read(content))

        built_by_identifier = {r.identifier: r for r in resources}
        updated: dict[str, Any] | list[dict[str, Any]]
        extra_files: dict[Path, str] = {}
        replacer = ResourceReplacer(value_by_placeholder, resource_io)
        if isinstance(source_dict_with_variable_substitution, dict) and isinstance(
            source_with_with_variable_placeholders, dict
        ):
            item_id = resource_io.get_id(source_dict_with_variable_substitution)
            updated = self._update(
                item_id,
                source_dict_with_variable_substitution,
                source_with_with_variable_placeholders,
                source_file,
                to_write,
                built_by_identifier,
                replacer,
                extra_files,
            )
        elif isinstance(source_dict_with_variable_substitution, list) and isinstance(
            source_with_with_variable_placeholders, list
        ):
            updated = []
            for i, source_dict_with_variable_substitution_i in enumerate(source_dict_with_variable_substitution):
                item_id = resource_io.get_id(source_dict_with_variable_substitution_i)
                updated.append(
                    self._update(
                        item_id,
                        source_dict_with_variable_substitution_i,
                        source_with_with_variable_placeholders[i],
                        source_file,
                        to_write,
                        built_by_identifier,
                        replacer,
                        extra_files,
                    )
                )
        else:
            raise ValueError("Loaded and loaded_with_ids should be of the same type")

        dumped = yaml_safe_dump(updated)
        for placeholder, variable in value_by_placeholder.items():
            dumped = dumped.replace(placeholder, f"{{{{ {variable.name} }}}}")
        file_content = comments.dump(dumped)
        return file_content, extra_files

    @staticmethod
    def _resolve_env_vars_in_variables(
        variables: list[BuildVariable], environment_variables: dict[str, str | None]
    ) -> list[BuildVariable]:
        """Substitute environment variable placeholders in build variable values.

        Replaces `${VAR_NAME}` references within string values or list-of-string
        values with their corresponding values from `environment_variables`.

        Args:
            variables: List of build variables that may contain environment variable syntax.
            environment_variables: Mapping of environment variable names to their values.

        Returns:
            A new list of `BuildVariable` instances with environment variables resolved.
        """
        variables_with_environment_list: list[BuildVariable] = []
        for variable in variables:
            updated_variable = variable
            if isinstance(updated_variable.value, str) and ENV_VAR_PATTERN.match(updated_variable.value):
                for key, value in environment_variables.items():
                    if key in updated_variable.value and isinstance(value, str):
                        # Running through all environment variables, in case multiple are used in the same variable.
                        updated_variable = updated_variable.model_copy(
                            update={"value": updated_variable.value.replace(f"${{{key}}}", value)}
                        )
            elif isinstance(variable.value, list):
                new_value: list[str | int | float | bool] = []
                for var_item in variable.value:
                    if isinstance(var_item, str) and ENV_VAR_PATTERN.match(var_item):
                        for key, value in environment_variables.items():
                            if key in var_item and isinstance(value, str):
                                var_item = var_item.replace(f"${{{key}}}", value)
                    new_value.append(var_item)
                updated_variable = variable.model_copy(update={"value": new_value})
            variables_with_environment_list.append(updated_variable)
        return variables_with_environment_list

    @classmethod
    def _update(
        cls,
        item_id: Identifier,
        source_with_variable_substitution: dict[str, Any],
        source_with_variable_placeholder: dict[str, Any],
        source_file: Path,
        to_write: dict[Identifier, dict[str, Any]],
        built_by_identifier: dict[Identifier, BuiltResource],
        replacer: "ResourceReplacer",
        extra_files: dict[Path, str],
    ) -> dict[str, Any]:
        if item_id not in to_write:
            raise ToolkitValueError(f"Bug in Toolkit resource {item_id} not found in to_write.")
        item_write = to_write[item_id]
        if item_id not in built_by_identifier:
            raise ToolkitMissingResourceError(f"Bug in Toolkit resource {item_id} not found in built resources.")
        built = built_by_identifier[item_id]

        if built.extra_files:
            for extra in built.extra_files:
                extra_content, extra_placeholders = BuildVariable.substitute_with_placeholders(
                    safe_read(extra.source_path), built.variables
                )
                if (
                    built.crud_cls.extra_content_property in item_write
                    and built.crud_cls.extra_content_property is not None
                ):
                    new_extra = item_write.pop(built.crud_cls.extra_content_property)
                    for placeholder, variable in extra_placeholders.items():
                        if placeholder in extra_content:
                            new_extra = new_extra.replace(str(variable.value), f"{{{{ {variable.name} }}}}")
                    extra_files[extra.source_path] = new_extra

        # Only split for resources that are sidecar-backed in build metadata, plus Skill (sidecar-first by design).
        if built.extra_files or replacer._loader.kind == "Skill":
            split_resources = list(replacer._loader.split_resource(source_file, item_write))
            base_to_write = item_write
            for split_path, split_content in split_resources:
                if split_path == source_file and isinstance(split_content, dict):
                    base_to_write = split_content
                elif isinstance(split_content, str):
                    extra_files[split_path] = split_content
            return replacer.replace(source_with_variable_substitution, source_with_variable_placeholder, base_to_write)

        return replacer.replace(source_with_variable_substitution, source_with_variable_placeholder, item_write)


class ResourceReplacer:
    """Replaces values in a local resource dictionary with the updated values from CDF.

    The local resource dict order is maintained. In addition, placeholders are used for variables.

    This class is responsible for merging CDF resource values back into local configuration files
    while preserving:
    - The original key ordering in dictionaries
    - Template variable placeholders (e.g., {{ variable_name }})
    - Comments and formatting where possible

    Args:
        value_by_placeholder: A mapping from placeholder strings to their corresponding
            BuildVariable objects. Placeholders are temporary substitutes for template
            variables during processing.
        loader: The ResourceCRUD loader instance used to determine how to diff lists
            and handle resource-specific logic.
    """

    def __init__(self, value_by_placeholder: dict[str, BuildVariable], loader: ResourceIO) -> None:
        self._value_by_placeholder = value_by_placeholder
        self._loader = loader

    def replace(
        self,
        current: dict[str, Any],
        placeholder: dict[str, Any],
        to_write: dict[str, Any],
    ) -> dict[str, Any]:
        """Replace values in a local resource dict with updated values from CDF.

        Merges the CDF resource values into the local configuration while maintaining
        the original dictionary key ordering and preserving template variable placeholders.

        Args:
            current: The current local resource dictionary with resolved variable values.
                This represents the source file content after template variables have
                been substituted with their actual values.
            placeholder: The local resource dictionary with placeholder strings instead
                of resolved values. Used to identify which values contain template
                variables that should be preserved.
            to_write: The resource dictionary from CDF containing the updated values
                to merge into the local configuration.

        Returns:
            A new dictionary with CDF values merged in, maintaining the original key
            order from `current`. Template variables are preserved as placeholders
            (to be converted back to {{ variable }} syntax by the caller). New keys
            from CDF are appended at the end, and removed keys are omitted.

        Raises:
            ToolkitValueError: If a list variable has changed and cannot be updated,
                or if there's a type mismatch between local and CDF values.
        """
        has_stringified_view_filter = False
        if isinstance(self._loader, ViewIO):
            # view.filter are recursive nested dicts that are complex. To avoid issues with comparing
            # lists inside the filters, we stringify them before processing such that they are compared
            # as strings.
            processed = []
            for d in (current, placeholder, to_write):
                if isinstance(d.get("filter"), dict):
                    d = d.copy()
                    d["filter"] = json.dumps(d["filter"])
                    has_stringified_view_filter = True
                processed.append(d)
            current, placeholder, to_write = processed
        output = self._replace_dict(current, placeholder, to_write, tuple())
        if has_stringified_view_filter and "filter" in output:
            # Special case for ViewCRUD where the filter is stringified in CDF
            output["filter"] = json.loads(output["filter"])
        return output

    def _replace_dict(
        self,
        current: dict[str, Any],
        placeholder: dict[str, Any],
        to_write: dict[str, Any],
        json_path: tuple[str | int, ...],
    ) -> dict[str, Any]:
        # Modified first to maintain original order
        # Then added, and skip removed
        updated: dict[str, Any] = {}
        variable_keys: set[str] = set()
        for modified_key, current_value in current.items():
            if modified_key not in to_write:
                # Removed item by skipping
                continue
            cdf_value = to_write[modified_key]

            if modified_key in placeholder:
                placeholder_value = placeholder[modified_key]
            elif variable_key := next(
                (
                    key
                    for key, variable in self._value_by_placeholder.items()
                    if key in placeholder and variable.value == modified_key
                ),
                None,
            ):
                # The key is a variable
                variable_keys.add(modified_key)
                modified_key = variable_key
                placeholder_value = placeholder[variable_key]
            else:
                # Bug in the code if this is reached, using a fallback.
                placeholder_value = current_value

            if isinstance(current_value, dict) and isinstance(cdf_value, dict):
                updated[modified_key] = self._replace_dict(
                    current_value, placeholder_value, cdf_value, (*json_path, modified_key)
                )
            elif isinstance(current_value, list) and isinstance(cdf_value, list):
                if isinstance(placeholder_value, str) and current_value == cdf_value:
                    # A list variable is used, and the list is unchanged.
                    updated[modified_key] = placeholder_value
                elif isinstance(placeholder_value, list):
                    updated[modified_key] = self._replace_list(
                        current_value, placeholder_value, cdf_value, (*json_path, modified_key)
                    )
                else:
                    # A list variable is used, but the list is changed. Since the value is represented as a single
                    # string, we cannot update it.
                    if variable := self._value_by_placeholder.get(placeholder_value):
                        raise ToolkitValueError(
                            f"Pull is not supported for list variable: {variable.name}: {variable.value}"
                        )
                    raise ToolkitValueError("Pull is not supported for list variable.")
            else:
                updated[modified_key] = self._replace_value(
                    current_value, placeholder_value, cdf_value, (*json_path, modified_key)
                )

        for new_key in to_write:
            if (new_key not in current) and new_key not in variable_keys:
                # Note there cannot be variables in new items
                updated[new_key] = to_write[new_key]
        return updated

    def _replace_list(
        self,
        current: list[Any],
        placeholder: list[Any],
        to_write: list[Any],
        json_path: tuple[str | int, ...],
    ) -> list[Any]:
        compare_indices, added_indices = self._loader.diff_list(current, to_write, json_path)
        updated: list[Any] = []
        for no, current_item in enumerate(current):
            if no not in compare_indices:
                # Removed item
                continue
            current_value = current_item
            placeholder_value = placeholder[no]
            cdf_value = to_write[compare_indices[no]]
            updated.append(self._replace_value(current_value, placeholder_value, cdf_value, (*json_path, no)))
        for added_index in added_indices:
            # Note there cannot be variables in new items
            updated.append(to_write[added_index])
        return updated

    def _replace_value(
        self,
        current: Any,
        placeholder_value: Any,
        to_write: Any,
        json_path: tuple[str | int, ...],
    ) -> Any:
        if isinstance(current, dict) and isinstance(to_write, dict):
            return self._replace_dict(current, placeholder_value, to_write, json_path)
        elif isinstance(current, list) and isinstance(to_write, list):
            return self._replace_list(current, placeholder_value, to_write, json_path)
        elif type(current) is type(to_write):
            if to_write == current:
                return placeholder_value
            if not isinstance(to_write, str):
                # Variable substitution is only supported for strings
                return to_write
            for placeholder, variable in self._value_by_placeholder.items():
                if placeholder in placeholder_value:
                    # We use the placeholder and not the {{ variable }} in the value to ensure
                    # that the result is valid yaml.
                    to_write = to_write.replace(variable.value, placeholder)  # type: ignore[arg-type]
                    # Iterate through all variables in case multiple are used in the same value.
            return to_write
        elif isinstance(current, dict) and isinstance(to_write, str):
            # This is a special case for the ExtractionPipelineConfigLoader where the config dict is typically a
            # dict locally, but returned as a string from the server.
            try:
                to_write = read_yaml_content(to_write)
            except yaml.YAMLError:
                ...
            else:
                return self._replace_dict(current, placeholder_value, to_write, json_path)

        raise ToolkitValueError(
            f"CDF value and local value should be of the same type in {'.'.join(map(str, json_path))}, "
            f"got {type(current)} != {type(to_write)}"
        )
