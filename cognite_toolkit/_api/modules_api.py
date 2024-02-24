from __future__ import annotations

import shutil
import tempfile
from collections.abc import Sequence
from pathlib import Path
from typing import Any, overload
from unittest.mock import MagicMock

import typer
from cognite.client import CogniteClient
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._api.data_classes import _DUMMY_ENVIRONMENT, Module, ModuleList
from cognite_toolkit._cdf import Common, clean, deploy
from cognite_toolkit._cdf_tk.load import ResourceTypes
from cognite_toolkit._cdf_tk.templates import COGNITE_MODULES, COGNITE_MODULES_PATH, build_config, iterate_modules
from cognite_toolkit._cdf_tk.templates.data_classes import BuildConfigYAML, Environment, InitConfigYAML, SystemYAML
from cognite_toolkit._cdf_tk.utils import CDFToolConfig


class ModulesAPI:
    def __init__(self, client: CogniteClient, url: str | None = None) -> None:
        self._client = client
        self._url = url
        self._build_dir = Path(tempfile.gettempdir()) / "cognite-toolkit" / "build"
        if self._build_dir.exists():
            shutil.rmtree(self._build_dir)
        self._build_dir.mkdir(parents=True, exist_ok=True)
        self.__module_by_name: dict[str, Module] = {}
        self._build_env = "dev"

    @property
    def _source_dir(self) -> Path:
        if self._url is not None:
            raise NotImplementedError("Loading modules from a URL is not yet supported")
        else:
            return COGNITE_MODULES_PATH

    def _load_modules(self) -> None:
        source_dir = self._source_dir

        system_yaml = SystemYAML.load_from_directory(source_dir, self._build_env)
        default_config = InitConfigYAML(_DUMMY_ENVIRONMENT).load_defaults(source_dir)

        for module, _ in iterate_modules(source_dir):
            module_packages = {
                package_name
                for package_name, package_modules in system_yaml.packages.items()
                if module.name in package_modules
            }
            self.__module_by_name[module.name] = Module._load(module, frozenset(module_packages), dict(default_config))

    @property
    def _modules_by_name(self) -> dict[str, Module]:
        if not self.__module_by_name:
            self._load_modules()
        return self.__module_by_name

    def _build(self, modules: Sequence[Module], verbose: bool) -> None:
        variables: dict[str, Any] = {COGNITE_MODULES: {}}
        for module in modules:
            key_parent_path = (COGNITE_MODULES, *module._source.relative_to(self._source_dir).parts)
            for variable in module.variables.values():
                key_path = (*key_parent_path, variable.name)
                current = variables
                for key in key_path[:-1]:
                    current = current.setdefault(key, {})
                current[key_path[-1]] = variable.value

        config = BuildConfigYAML(
            environment=Environment(
                name=self._build_env,
                project=self._client.config.project,
                build_type=self._build_env,
                selected_modules_and_packages=[module.name for module in modules],
                common_function_code="./common_function_code",
            ),
            filepath=Path(""),
            modules=variables,
        )
        build_config(
            self._build_dir,
            self._source_dir,
            config,
            system_config=SystemYAML.load_from_directory(self._source_dir, self._build_env),
            clean=True,
            verbose=verbose,
        )

    def _create_context(self, verbose: bool) -> typer.Context:
        context = MagicMock(spec=typer.Context)
        cluster = self._client.config.base_url.removeprefix("https://").split(".", maxsplit=1)[0]
        cdf_tool_config = CDFToolConfig(cluster=cluster, project=self._client.config.project)
        cdf_tool_config._client = self._client

        context.obj = Common(
            verbose=verbose,
            override_env=True,
            cluster=cluster,
            project=self._client.config.project,
            mockToolGlobals=cdf_tool_config,
        )
        return context

    def deploy(
        self,
        module: Module | Sequence[Module],
        include: set[ResourceTypes] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        modules = [module] if isinstance(module, Module) else module
        self._build(modules, verbose)
        ctx = self._create_context(verbose)

        deploy(
            ctx=ctx,
            build_dir=str(self._build_dir),
            build_env=self._build_env,
            interactive=False,
            drop=False,
            drop_data=False,
            dry_run=dry_run,
            include=list(include) if include is not None else None,
        )

    def clean(
        self,
        module: Module | Sequence[Module],
        include: set[ResourceTypes] | None = None,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        modules = [module] if isinstance(module, Module) else module
        self._build(modules, verbose)
        ctx = self._create_context(verbose)

        clean(
            ctx=ctx,
            build_dir=str(self._build_dir),
            build_env=self._build_env,
            interactive=False,
            dry_run=dry_run,
            include=list(include) if include is not None else None,
        )

    @overload
    def retrieve(self, module: str) -> Module: ...

    @overload
    def retrieve(self, module: SequenceNotStr[str]) -> ModuleList: ...

    def retrieve(self, module: str | SequenceNotStr[str]) -> Module | ModuleList:
        if isinstance(module, str):
            return self._modules_by_name[module]
        else:
            return ModuleList([self._modules_by_name[modul] for modul in module])

    def list(self) -> ModuleList:
        return ModuleList(self._modules_by_name.values())
