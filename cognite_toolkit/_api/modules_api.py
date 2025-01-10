from __future__ import annotations

import os
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any, Literal, cast, overload
from unittest.mock import MagicMock

import typer
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._api.data_classes import _DUMMY_ENVIRONMENT, ModuleMeta, ModuleMetaList
from cognite_toolkit._cdf_tk.apps._core_app import Common, CoreApp
from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.commands.build import BuildCommand
from cognite_toolkit._cdf_tk.constants import BUILTIN_MODULES_PATH, MODULES
from cognite_toolkit._cdf_tk.data_classes import BuildConfigYAML, Environment, InitConfigYAML
from cognite_toolkit._cdf_tk.loaders import ResourceTypes
from cognite_toolkit._cdf_tk.utils import iterate_modules
from cognite_toolkit._cdf_tk.utils.file import safe_rmtree


class ModulesAPI:
    def __init__(self, project_name: str, url: str | None = None) -> None:
        self._project_name = project_name
        self._url = url
        try:
            pid = os.getpid()
        except AttributeError:
            pid = 0
        self._build_dir = Path(tempfile.gettempdir()) / "cognite-toolkit" / f"build-{pid}"
        if self._build_dir.exists():
            safe_rmtree(self._build_dir)
        self._build_dir.mkdir(parents=True, exist_ok=True)
        self.__module_by_name: dict[str, ModuleMeta] = {}
        self._build_env = "dev"

    def _source_dir(self) -> Path:
        if self._url is not None:
            raise NotImplementedError("Loading modules from a URL is not yet supported")
        else:
            return BUILTIN_MODULES_PATH

    def _load_modules(self) -> None:
        organization_dir = self._source_dir()

        cdf_toml = CDFToml.load(organization_dir.parent)
        default_config = InitConfigYAML(_DUMMY_ENVIRONMENT).load_defaults(organization_dir)

        for module, _ in iterate_modules(organization_dir):
            module_packages = {
                package_name
                for package_name, package_modules in cdf_toml.modules.packages.items()
                if module.name in package_modules
            }
            self.__module_by_name[module.name] = ModuleMeta._load(
                module, frozenset(module_packages), dict(default_config)
            )

    @property
    def _modules_by_name(self) -> dict[str, ModuleMeta]:
        if not self.__module_by_name:
            self._load_modules()
        return self.__module_by_name

    def _build(self, modules: Sequence[ModuleMeta], verbose: bool) -> None:
        variables: dict[str, Any] = {MODULES: {}}
        for module in modules:
            key_parent_path = (MODULES, *module._source.relative_to(self._source_dir()).parts)
            for variable in module.variables.values():
                key_path = (*key_parent_path, variable.name)
                current = variables
                for key in key_path[:-1]:
                    current = current.setdefault(key, {})
                current[key_path[-1]] = variable.value

        config = BuildConfigYAML(
            environment=Environment(
                name=self._build_env,
                project=self._project_name,
                build_type=cast(Literal["dev"], self._build_env),
                selected=[module.name for module in modules],
            ),
            filepath=Path(""),
            variables=variables,
        )
        BuildCommand().build_config(
            self._build_dir,
            self._source_dir().parent,
            config,
            packages=CDFToml.load(self._source_dir().parent).modules.packages,
            clean=True,
            verbose=verbose,
        )

    def _create_context(self, verbose: bool) -> typer.Context:
        context = MagicMock(spec=typer.Context)
        context.obj = Common(
            # This means that any variables found in an .env file will NOT override the current environment variables.
            override_env=False,
            # These are only for CLI override, they are picked up from environment or .env file or context in pyodide notebook.
            mockToolGlobals=None,
        )
        return context

    def deploy(
        self,
        module: ModuleMeta | Sequence[ModuleMeta],
        include: set[ResourceTypes] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        modules = [module] if isinstance(module, ModuleMeta) else module
        self._build(modules, verbose)
        ctx = self._create_context(verbose)
        app = CoreApp()
        app.deploy(
            ctx=ctx,
            build_dir=self._build_dir,
            build_env_name=self._build_env,
            drop=False,
            drop_data=False,
            dry_run=dry_run,
            include=list(include) if include is not None else None,
        )

    def clean(
        self,
        module: ModuleMeta | Sequence[ModuleMeta],
        include: set[ResourceTypes] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        modules = [module] if isinstance(module, ModuleMeta) else module
        self._build(modules, verbose)
        ctx = self._create_context(verbose)
        app = CoreApp()
        app.clean(
            ctx=ctx,
            build_dir=self._build_dir,
            build_env_name=self._build_env,
            dry_run=dry_run,
            include=list(include) if include is not None else None,
        )

    @overload
    def retrieve(self, module: str) -> ModuleMeta: ...

    @overload
    def retrieve(self, module: SequenceNotStr[str]) -> ModuleMetaList: ...

    def retrieve(self, module: str | SequenceNotStr[str]) -> ModuleMeta | ModuleMetaList:
        if isinstance(module, str):
            return self._modules_by_name[module]
        else:
            return ModuleMetaList([self._modules_by_name[modul] for modul in module])

    def list(self) -> ModuleMetaList:
        return ModuleMetaList(self._modules_by_name.values())
