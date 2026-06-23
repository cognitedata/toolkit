import contextlib
import io
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Literal, TypeAlias, cast

from rich.console import Console
from rich.tree import Tree

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._resource_base import Identifier, T_Identifier, T_ResponseResource
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_v2.build_v2 import BuildV2Command
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes import BuildFolder, BuildParameters, BuiltModule
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._build import BuiltResource
from cognite_toolkit._cdf_tk.commands.deploy_v2.command import DeployOptions, DeployV2Command, ReadResource
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.resource_ios import ResourceIO
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables
from cognite_toolkit._cdf_tk.utils.file import relative_to_if_possible

ResourceKey: TypeAlias = tuple[type[ResourceIO], Identifier]
ResourceStatus: TypeAlias = Literal["new", "unchanged", "changed", "unknown"]
CDFStatus: TypeAlias = bool | Literal["unknown"]
StatusFormat: TypeAlias = Literal["tree", "json"]


@dataclass(frozen=True)
class StatusDependency:
    crud_cls: type[ResourceIO]
    identifier: Identifier
    in_config: bool
    in_cdf: CDFStatus

    @property
    def key(self) -> ResourceKey:
        return self.crud_cls, self.identifier

    def as_dict(self) -> dict[str, Any]:
        return {
            "resource_type": self.crud_cls.folder_name,
            "kind": self.crud_cls.kind,
            "identifier": self.identifier.dump(),
            "in_config": self.in_config,
            "in_cdf": self.in_cdf,
        }


@dataclass
class StatusResource:
    crud_cls: type[ResourceIO]
    identifier: Identifier
    module_path: Path
    source_path: Path
    status: ResourceStatus
    dependencies: list[StatusDependency] = field(default_factory=list)

    @property
    def key(self) -> ResourceKey:
        return self.crud_cls, self.identifier

    def as_dict(self) -> dict[str, Any]:
        return {
            "resource_type": self.crud_cls.folder_name,
            "kind": self.crud_cls.kind,
            "identifier": self.identifier.dump(),
            "status": self.status,
            "module_path": self.module_path.as_posix(),
            "source_path": self.source_path.as_posix(),
            "dependencies": [dependency.as_dict() for dependency in self.dependencies],
        }


@dataclass
class StatusGraph:
    resources: list[StatusResource]

    @property
    def resources_by_key(self) -> dict[ResourceKey, StatusResource]:
        return {resource.key: resource for resource in self.resources}

    def topological_resources(self) -> list[StatusResource]:
        resources_by_key = self.resources_by_key
        dependencies_by_key = {
            resource.key: {dependency.key for dependency in resource.dependencies if dependency.key in resources_by_key}
            for resource in self.resources
        }
        try:
            ordered = list(TopologicalSorter(dependencies_by_key).static_order())
        except CycleError:
            return self.resources
        return [resources_by_key[key] for key in ordered if key in resources_by_key]

    def root_resources(self) -> list[StatusResource]:
        resources_by_key = self.resources_by_key
        depended_on = {
            dependency.key
            for resource in self.resources
            for dependency in resource.dependencies
            if dependency.key in resources_by_key
        }
        roots = [resource for resource in self.topological_resources() if resource.key not in depended_on]
        return roots or self.topological_resources()

    def as_dict(self) -> dict[str, Any]:
        return {"resources": [resource.as_dict() for resource in self.topological_resources()]}


class StatusCommand(ToolkitCommand):
    def execute(
        self,
        env_vars: EnvironmentVariables,
        organization_dir: Path,
        config_yaml: Path | None,
        selected: list[str] | None,
        output_format: StatusFormat,
        verbose: bool,
        client: ToolkitClient | None = None,
    ) -> StatusGraph:
        client = client or self._client
        console = client.console if client else Console(markup=True)
        with TemporaryDirectory(prefix="cdf-status-") as tmp_dir:
            build_parameters = BuildParameters(
                organization_dir=organization_dir,
                build_dir=Path(tmp_dir),
                config_yaml=config_yaml,
                user_selected_modules=selected or ["modules/"],
                verbose=verbose,
                insight_format="json",
            )
            build = self._run_build_silently(build_parameters, client)
            graph = self.build_graph(build.built_modules, client, env_vars)

        if output_format == "json":
            console.print(json.dumps(graph.as_dict(), indent=2))
        else:
            console.print(self.render_tree(graph))
        return graph

    def _run_build_silently(self, build_parameters: BuildParameters, client: ToolkitClient | None) -> BuildFolder:
        stdout = io.StringIO()
        stderr = io.StringIO()
        hidden_console = Console(file=io.StringIO(), markup=True)
        original_console = client.console if client else None
        if client:
            client.console = hidden_console
        try:
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                return BuildV2Command(print_warning=False, skip_tracking=True).build(build_parameters, client=None)
        finally:
            if client and original_console:
                client.console = original_console

    @classmethod
    def build_graph(
        cls,
        modules: Iterable[BuiltModule],
        client: ToolkitClient | None,
        env_vars: EnvironmentVariables,
    ) -> StatusGraph:
        built_resources_by_key: dict[ResourceKey, BuiltResource] = {}
        module_path_by_key: dict[ResourceKey, Path] = {}
        resources_by_crud: dict[type[ResourceIO], list[BuiltResource]] = defaultdict(list)
        for module in modules:
            for resource in module.resources:
                key = (resource.crud_cls, resource.identifier)
                built_resources_by_key[key] = resource
                module_path_by_key[key] = module.module_id.id
                resources_by_crud[resource.crud_cls].append(resource)

        status_by_key = cls._get_resource_statuses(resources_by_crud, client, env_vars)
        dependency_cdf_presence = cls._get_dependency_cdf_presence(built_resources_by_key.values(), client)
        status_resources: list[StatusResource] = []
        for key, built_resource in built_resources_by_key.items():
            dependencies = [
                StatusDependency(
                    crud_cls=crud_cls,
                    identifier=identifier,
                    in_config=(crud_cls, identifier) in built_resources_by_key,
                    in_cdf=dependency_cdf_presence.get((crud_cls, identifier), False),
                )
                for crud_cls, identifier in sorted(
                    built_resource.dependencies, key=lambda item: (item[0].folder_name, item[0].kind, str(item[1]))
                )
            ]
            status_resources.append(
                StatusResource(
                    crud_cls=built_resource.crud_cls,
                    identifier=built_resource.identifier,
                    module_path=module_path_by_key[key],
                    source_path=built_resource.source_path,
                    status=status_by_key.get(key, "new"),
                    dependencies=dependencies,
                )
            )

        return StatusGraph(status_resources)

    @classmethod
    def _get_resource_statuses(
        cls,
        resources_by_crud: Mapping[type[ResourceIO], list[BuiltResource]],
        client: ToolkitClient | None,
        env_vars: EnvironmentVariables,
    ) -> dict[ResourceKey, ResourceStatus]:
        statuses: dict[ResourceKey, ResourceStatus] = {}
        if client is None:
            return {
                (resource.crud_cls, resource.identifier): "unknown"
                for resources in resources_by_crud.values()
                for resource in resources
            }
        options = DeployOptions(dry_run=True, environment_variables=env_vars.dump())
        for crud_cls, resources in resources_by_crud.items():
            try:
                crud = crud_cls.create_loader(client)
                resource_by_id = DeployV2Command._read_resource_files(
                    crud, [resource.build_path for resource in resources], options
                )
                if not resource_by_id:
                    continue
                cdf_by_id = cls._retrieve_by_id(crud, resource_by_id.keys())
            except Exception:
                for resource in resources:
                    statuses[(resource.crud_cls, resource.identifier)] = "unknown"
                continue
            for identifier, read_resource in resource_by_id.items():
                statuses[(crud_cls, identifier)] = cls._classify_resource(crud, identifier, read_resource, cdf_by_id)
        return statuses

    @classmethod
    def _classify_resource(
        cls,
        crud: ResourceIO[T_Identifier, Any, T_ResponseResource],
        identifier: T_Identifier,
        read_resource: ReadResource[Any],
        cdf_by_id: Mapping[T_Identifier, T_ResponseResource],
    ) -> ResourceStatus:
        cdf_resource = cdf_by_id.get(identifier)
        if cdf_resource is None:
            return "new"
        cdf_dict = crud.dump_resource(cdf_resource, read_resource.raw_dict)
        if cdf_dict == read_resource.raw_dict:
            return "unchanged"
        return "changed"

    @classmethod
    def _get_dependency_cdf_presence(
        cls,
        resources: Iterable[BuiltResource],
        client: ToolkitClient | None,
    ) -> dict[ResourceKey, CDFStatus]:
        identifiers_by_crud: dict[type[ResourceIO], set[Identifier]] = defaultdict(set)
        for resource in resources:
            for crud_cls, identifier in resource.dependencies:
                identifiers_by_crud[crud_cls].add(identifier)

        if client is None:
            return {
                (crud_cls, identifier): "unknown"
                for crud_cls, identifiers in identifiers_by_crud.items()
                for identifier in identifiers
            }

        exists_by_key: dict[ResourceKey, CDFStatus] = {}
        for crud_cls, identifiers in identifiers_by_crud.items():
            try:
                crud = crud_cls.create_loader(client)
                existing = set(cls._retrieve_by_id(crud, identifiers).keys())
            except Exception:
                for identifier in identifiers:
                    exists_by_key[(crud_cls, identifier)] = "unknown"
                continue
            for identifier in identifiers:
                exists_by_key[(crud_cls, identifier)] = identifier in existing
        return exists_by_key

    @staticmethod
    def _retrieve_by_id(
        crud: ResourceIO[T_Identifier, Any, T_ResponseResource],
        identifiers: Iterable[T_Identifier],
    ) -> dict[T_Identifier, T_ResponseResource]:
        identifier_list = list(identifiers)
        if not identifier_list:
            return {}
        return {crud.get_id(resource): resource for resource in crud.retrieve(identifier_list)}

    @classmethod
    def render_tree(cls, graph: StatusGraph) -> Tree:
        tree = Tree("[bold]CDF status[/]")
        resources_by_key = graph.resources_by_key
        for resource in graph.root_resources():
            cls._add_resource_branch(tree, resource, resources_by_key, set())
        return tree

    @classmethod
    def _add_resource_branch(
        cls,
        tree: Tree,
        resource: StatusResource,
        resources_by_key: Mapping[ResourceKey, StatusResource],
        seen: set[ResourceKey],
    ) -> None:
        label = cls._resource_label(resource)
        branch = tree.add(label)
        if resource.key in seen:
            branch.add("[dim]Already shown above[/]")
            return

        next_seen = {*seen, resource.key}
        for dependency in resource.dependencies:
            dependency_resource = resources_by_key.get(dependency.key)
            if dependency_resource is None:
                branch.add(cls._dependency_label(dependency))
            else:
                cls._add_resource_branch(branch, dependency_resource, resources_by_key, next_seen)

    @staticmethod
    def _resource_label(resource: StatusResource) -> str:
        status_style = {
            "new": "green",
            "unchanged": "dim",
            "changed": "yellow",
            "unknown": "magenta",
        }[resource.status]
        module_path = relative_to_if_possible(resource.module_path)
        return (
            f"[{status_style}]{resource.status}[/] "
            f"[bold]{resource.crud_cls.kind}[/] {resource.identifier} "
            f"[dim](module: {module_path.as_posix()})[/]"
        )

    @staticmethod
    def _dependency_label(dependency: StatusDependency) -> str:
        in_config = "yes" if dependency.in_config else "no"
        in_cdf = "unknown" if dependency.in_cdf == "unknown" else "yes" if dependency.in_cdf else "no"
        config_style = "green" if dependency.in_config else "red"
        cdf_style = "magenta" if dependency.in_cdf == "unknown" else "green" if dependency.in_cdf else "red"
        return (
            f"[bold]{dependency.crud_cls.kind}[/] {dependency.identifier} "
            f"[{config_style}]in_config={in_config}[/] [{cdf_style}]in_cdf={in_cdf}[/]"
        )


def validate_status_format(value: str) -> StatusFormat:
    if value not in ("tree", "json"):
        raise ToolkitValueError("Invalid status format. Expected 'tree' or 'json'.")
    return cast(StatusFormat, value)
