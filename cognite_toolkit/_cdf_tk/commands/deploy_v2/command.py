from collections.abc import Sequence
from dataclasses import dataclass, field
from graphlib import TopologicalSorter
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.cruds import CRUDS_BY_FOLDER_NAME, DataCRUD, Loader
from cognite_toolkit._cdf_tk.exceptions import ToolkitNotADirectoryError, ToolkitValidationError
from cognite_toolkit._cdf_tk.tk_warnings.other import ToolkitDependenciesIncludedWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


@dataclass
class DeployOptions:
    dry_run: bool = False
    include: Sequence[str] | None = None
    force_update: bool = False
    verbose: bool = False


@dataclass
class DeploymentStep:
    loader_cls: type[Loader]
    files: list[Path]


@dataclass
class DeploymentPlan:
    steps: list[DeploymentStep] = field(default_factory=list)


@dataclass
class DeploymentResult: ...


class DeployV2Command(ToolkitCommand):
    def deploy(
        self,
        env_vars: EnvironmentVariables,
        build_dir: Path,
        options: DeployOptions | None = None,
    ) -> Any:
        self._validate_user_input(build_dir, options)
        options = options or DeployOptions()
        plan = self._create_deployment_plan(build_dir, options.include)

        self._display_plan(plan)

        results = self._apply_plan(env_vars, plan, options.dry_run, options.force_update)

        self._display_results(results)

        return results

    @classmethod
    def _validate_user_input(cls, build_dir: Path, options: DeployOptions | None = None) -> None:
        if not build_dir.is_dir():
            raise ToolkitNotADirectoryError(f"Build directory {build_dir!s} does not exist.")
        if options and options.include:
            available = set(CRUDS_BY_FOLDER_NAME)
            if invalid := set(options.include) - available:
                raise ToolkitValidationError(
                    f"Invalid resource types specified: {humanize_collection(invalid)}, available types: {humanize_collection(available)}"
                )

    def _create_deployment_plan(self, build_dir: Path, include: Sequence[str] | None = None) -> DeploymentPlan:
        selected = self._select_loaders(build_dir, include)
        ordered = self._topological_sort(selected, build_dir)

        steps: list[DeploymentStep] = []
        for loader_cls in ordered:
            resource_dir = build_dir / loader_cls.folder_name
            if resource_dir.is_dir():
                files = sorted(f for f in resource_dir.rglob("*") if loader_cls.is_supported_file(f))
            else:
                files = []
            steps.append(DeploymentStep(loader_cls=loader_cls, files=files))

        return DeploymentPlan(steps=steps)

    @staticmethod
    def _select_loaders(build_dir: Path, include: Sequence[str] | None) -> dict[type[Loader], frozenset[type[Loader]]]:
        selected: dict[type[Loader], frozenset[type[Loader]]] = {}
        for folder_name, loader_classes in CRUDS_BY_FOLDER_NAME.items():
            if include is not None and folder_name not in include:
                continue
            if not (build_dir / folder_name).is_dir():
                continue
            for loader_cls in loader_classes:
                if loader_cls.any_supported_files(build_dir / folder_name):
                    selected[loader_cls] = loader_cls.dependencies
                elif issubclass(loader_cls, DataCRUD):
                    selected[loader_cls] = loader_cls.dependencies
        return selected

    def _topological_sort(
        self, selected: dict[type[Loader], frozenset[type[Loader]]], build_dir: Path
    ) -> list[type[Loader]]:
        ordered: list[type[Loader]] = []
        should_include: list[type[Loader]] = []
        for loader_cls in TopologicalSorter(selected).static_order():
            if loader_cls in selected:
                ordered.append(loader_cls)
            elif (build_dir / loader_cls.folder_name).is_dir():
                should_include.append(loader_cls)
        if should_include:
            self.warn(ToolkitDependenciesIncludedWarning(list({item.folder_name for item in should_include})))
        return ordered

    def _display_plan(self, plan: DeploymentPlan) -> None:
        raise NotImplementedError()

    def _apply_plan(
        self, env_vars: EnvironmentVariables, plan: DeploymentPlan, dry_run: bool, force_update: bool
    ) -> Sequence[DeploymentResult]:
        raise NotImplementedError()

    def _display_results(self, results: Sequence[DeploymentResult]) -> None:
        raise NotImplementedError()
