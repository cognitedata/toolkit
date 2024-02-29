from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, ClassVar

from rich import print

from cognite_toolkit._cdf_tk.templates.data_classes._base import ConfigCore, _load_version_variable


@dataclass
class SystemYAML(ConfigCore):
    file_name: ClassVar[str] = "_system.yaml"
    cdf_toolkit_version: str
    packages: dict[str, list[str]] = field(default_factory=dict)

    @classmethod
    def _file_name(cls, build_env: str) -> str:
        return cls.file_name

    @classmethod
    def load(cls, data: dict[str, Any], build_env: str, filepath: Path) -> SystemYAML:
        version = _load_version_variable(data, filepath.name)
        packages = data.get("packages", {})
        if not packages:
            print(f"  [bold yellow]Warning:[/] No packages defined in {cls.file_name}.")
        return cls(
            filepath=filepath,
            cdf_toolkit_version=version,
            packages=packages,
        )

    def validate_modules(self, available_modules: set[str], selected_modules_and_packages: list[str]) -> None:
        selected_packages = {package for package in selected_modules_and_packages if package in self.packages}
        for package, modules in self.packages.items():
            if package not in selected_packages:
                # We do not check packages that are not selected.
                # Typically, the user will delete the modules that are irrelevant for them,
                # thus we only check the selected packages.
                continue
            if missing := set(modules) - available_modules:
                print(
                    f"  [bold red]ERROR:[/] Package {package} defined in {self.filepath.name!s} is referring "
                    f"the following missing modules {missing}."
                )
                exit(1)
