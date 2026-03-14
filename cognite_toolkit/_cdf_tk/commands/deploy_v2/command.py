from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.utils.auth import EnvironmentVariables


@dataclass
class DeployOptions:
    dry_run: bool = False
    include: Sequence[str] | None = None
    force_update: bool = False
    verbose: bool = False


@dataclass
class DeploymentPlan: ...


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

    def _validate_user_input(self, build_dir: Path, options: DeployOptions | None = None) -> None:
        raise NotImplementedError()

    def _create_deployment_plan(self, build_dir: Path, include: Sequence[str] | None = None) -> DeploymentPlan:
        raise NotImplementedError()

    def _display_plan(self, plan: DeploymentPlan) -> None:
        raise NotImplementedError()

    def _apply_plan(
        self, env_vars: EnvironmentVariables, plan: DeploymentPlan, dry_run: bool, force_update: bool
    ) -> Sequence[DeploymentResult]:
        raise NotImplementedError()

    def _display_results(self, results: Sequence[DeploymentResult]) -> None:
        raise NotImplementedError()
