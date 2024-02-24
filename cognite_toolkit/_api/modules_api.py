from __future__ import annotations

from collections.abc import Sequence
from typing import overload

from cognite.client import CogniteClient
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._api.data_classes import _DUMMY_ENVIRONMENT, Module, ModuleList
from cognite_toolkit._cdf_tk.load import ResourceTypes
from cognite_toolkit._cdf_tk.templates import COGNITE_MODULES_PATH, iterate_modules
from cognite_toolkit._cdf_tk.templates.data_classes import InitConfigYAML, SystemYAML


class ModulesAPI:
    def __init__(self, client: CogniteClient, url: str | None = None) -> None:
        self._client = client
        self._url = url
        self.__module_by_name: dict[str, Module] = {}

    def _load_modules(self) -> None:
        if self._url is not None:
            raise NotImplementedError("Loading modules from a URL is not yet supported")
        else:
            modules_path = COGNITE_MODULES_PATH

        system_yaml = SystemYAML.load_from_directory(COGNITE_MODULES_PATH, "does not matter")
        default_config = InitConfigYAML(_DUMMY_ENVIRONMENT).load_defaults(COGNITE_MODULES_PATH)

        for module, _ in iterate_modules(modules_path):
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

    def deploy(
        self,
        module: Module | Sequence[Module],
        include: set[ResourceTypes] | None,
        exclude: set[ResourceTypes] | None = None,
    ) -> Module | ModuleList:
        if include is not None and exclude is not None:
            raise ValueError("Cannot specify both resources to include and exclude")

        if isinstance(module, Module):
            module = [module]

        raise NotImplementedError

    def clean(
        self,
        module: Module | Sequence[Module],
        include: set[ResourceTypes] | None,
        exclude: set[ResourceTypes] | None = None,
    ) -> Module | ModuleList:
        raise NotImplementedError

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
