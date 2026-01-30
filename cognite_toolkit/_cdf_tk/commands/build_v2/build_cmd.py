import re
from itertools import groupby
from pathlib import Path
from typing import Any, Literal

from rich import print
from rich.panel import Panel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.commands._base import ToolkitCommand
from cognite_toolkit._cdf_tk.commands.build_cmd import BuildCommand as OldBuildCommand
from cognite_toolkit._cdf_tk.commands.build_v2._modules_parser import ModulesParser
from cognite_toolkit._cdf_tk.commands.build_v2.build_parameters import BuildParameters
from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._modules import Module
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, BuildVariables, BuiltModuleList
from cognite_toolkit._cdf_tk.data_classes._issues import Issue, IssueList
from cognite_toolkit._cdf_tk.exceptions import ToolkitError
from cognite_toolkit._cdf_tk.tk_warnings import ToolkitWarning, WarningList
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree
from cognite_toolkit._version import __version__


class BuildCommand(ToolkitCommand):
    def __init__(
        self,
        print_warning: bool = True,
        skip_tracking: bool = False,
        silent: bool = False,
        client: ToolkitClient | None = None,
    ) -> None:
        super().__init__(print_warning, skip_tracking, silent, client)
        self.issues = IssueList()

    def execute(
        self,
        verbose: bool,
        base_dir: Path,
        build_dir: Path,
        selected: list[str | Path] | None,
        build_env: str | None,
        no_clean: bool,
        client: ToolkitClient | None = None,
        on_error: Literal["continue", "raise"] = "continue",
    ) -> BuiltModuleList:
        """
        Build the resources into deployable artifacts in the build directory.
        """

        self.verbose = verbose
        self.on_error = on_error

        build_parameters = BuildParameters.load(
            organization_dir=base_dir,
            build_dir=build_dir,
            build_env_name=build_env,
            client=client,
            user_selected=selected,
        )

        # Print the build input.
        if self.verbose:
            self._print_build_input(build_parameters)

        # Tracking the project and cluster for the build.
        if build_parameters.client:
            self._additional_tracking_info.project = build_parameters.client.config.project
            self._additional_tracking_info.cluster = build_parameters.client.config.cdf_cluster

        # Load modules
        modules_parser = ModulesParser(organization_dir=base_dir, selected=selected)
        module_paths = modules_parser.parse()
        module_loading_issues = modules_parser.issues
        if module_loading_issues:
            self.issues.extend(module_loading_issues)
            self._print_or_log_issues_by_category(self.issues)
            raise ToolkitError("Module loading issues encountered. Cannot continue. See above for details.")

        # Load modules
        if module_paths:
            pass

        # modules = [Module.load(path) for path in module_paths]
        # for module in modules:
        #    continue

        # Logistics: clean and create build directory
        if prepare_issues := self._prepare_target_directory(build_dir, not no_clean):
            self.issues.extend(prepare_issues)

        # Compile the configuration and variables,
        # check syntax on module and resource level
        # for any "compilation errors and warnings"
        built_modules, build_integrity_issues = self._build_configuration(build_parameters)
        if build_integrity_issues:
            self.issues.extend(build_integrity_issues)

        # This is where we would add any recommendations for the user to improve the build.
        if build_quality_issues := self._verify_build_quality(built_modules):
            self.issues.extend(build_quality_issues)

        # Finally, print warnings grouped by category/code and location.
        self._print_or_log_issues_by_category(self.issues)

        return built_modules

    def _print_build_input(self, build_input: BuildParameters) -> None:
        print(
            Panel(
                f"Building {build_input.organization_dir!s}:\n  - Toolkit Version '{__version__!s}'\n"
                f"  - Environment name {build_input.build_env_name!r}, validation-type {build_input.config.environment.validation_type!r}.\n"
                f"  - Config '{build_input.config.filepath!s}'",
                expand=False,
            )
        )

    def _prepare_target_directory(self, build_dir: Path, clean: bool = False) -> IssueList:
        """
        Directory logistics
        """
        issues = IssueList()
        if build_dir.exists() and any(build_dir.iterdir()):
            if not clean:
                raise ToolkitError("Build directory is not empty. Run without --no-clean to remove existing files.")

            if self.verbose:
                issues.append(Issue(code="BUILD_001"))
            safe_rmtree(build_dir)
        build_dir.mkdir(parents=True, exist_ok=True)
        return issues

    def _build_configuration(self, build_input: BuildParameters) -> tuple[BuiltModuleList, IssueList]:
        issues = IssueList()
        # Use build_input.modules directly (it is already filtered by selection)

        old_build_command = OldBuildCommand(print_warning=False, skip_tracking=False)
        built_modules = old_build_command.build_config(
            build_dir=build_input.build_dir,
            organization_dir=build_input.organization_dir,
            config=build_input.config,
            packages={},
            clean=False,
            verbose=self.verbose,
            client=build_input.client,
            progress_bar=False,
            on_error=self.on_error,
        )
        # Copy tracking info from old command to self
        self._additional_tracking_info.package_ids.update(old_build_command._additional_tracking_info.package_ids)
        self._additional_tracking_info.module_ids.update(old_build_command._additional_tracking_info.module_ids)

        # Collect warnings from the old build command and convert to issues
        # Always convert warnings to issues, even if the list appears empty
        # (WarningList might have custom __bool__ behavior)
        if old_build_command.warning_list:
            converted_issues = IssueList.from_warning_list(old_build_command.warning_list)
            issues.extend(converted_issues)
        return built_modules, issues

    def _verify_build_quality(self, built_modules: BuiltModuleList) -> IssueList:
        issues = IssueList()
        return issues

    def _write(self, build_input: BuildParameters) -> None:
        # Write the build to the build directory.
        # Track lines of code built.
        raise NotImplementedError()

    def _track(self, build_input: BuildParameters) -> None:
        raise NotImplementedError()

    def _print_or_log_issues_by_category(self, issues: IssueList) -> None:
        issues_sorted = sorted(issues, key=self._issue_sort_key)
        for code, grouped_issues in groupby(issues_sorted, key=lambda issue: issue.code or ""):
            print(f"[bold]{code}[/]")
            for issue in grouped_issues:
                message = issue.message or ""
                print(f"  - {message}")

    def _issue_sort_key(self, issue: Issue) -> tuple[str, str]:
        code = issue.code or ""
        if not issue.message:
            return code, ""
        match = re.search(r"'([^']+)'", issue.message)
        path = match.group(1) if match else issue.message
        return code, path

    # Delegate to old BuildCommand for backward compatibility with tests
    def build_modules(
        self,
        modules: list[Module],
        build_dir: Path,
        variables: BuildVariables,
        verbose: bool = False,
        progress_bar: bool = False,
        on_error: Literal["continue", "raise"] = "continue",
    ) -> BuiltModuleList:
        """Delegate to old BuildCommand for backward compatibility."""
        old_cmd = OldBuildCommand()

        built_modules = old_cmd.build_modules(modules, build_dir, variables, verbose, progress_bar, on_error)  # type: ignore[arg-type]
        self._additional_tracking_info.package_ids.update(old_cmd._additional_tracking_info.package_ids)
        self._additional_tracking_info.module_ids.update(old_cmd._additional_tracking_info.module_ids)
        self.issues.extend(IssueList.from_warning_list(old_cmd.warning_list or WarningList[ToolkitWarning]()))
        return built_modules

    def build_config(
        self,
        build_dir: Path,
        organization_dir: Path,
        config: BuildConfigYAML,
        packages: dict[str, list[str]],
        clean: bool = False,
        verbose: bool = False,
        client: ToolkitClient | None = None,
        progress_bar: bool = False,
        on_error: Literal["continue", "raise"] = "continue",
    ) -> BuiltModuleList:
        """Delegate to old BuildCommand for backward compatibility."""
        old_cmd = OldBuildCommand()
        return old_cmd.build_config(
            build_dir, organization_dir, config, packages, clean, verbose, client, progress_bar, on_error
        )

    def _replace_variables(
        self,
        resource_files: list[Path],
        variables: BuildVariables,
        resource_name: str,
        module_dir: Path,
        verbose: bool = False,
    ) -> list[Any]:
        """Delegate to old BuildCommand for backward compatibility."""
        old_cmd = OldBuildCommand()
        return old_cmd._replace_variables(resource_files, variables, resource_name, module_dir, verbose)
