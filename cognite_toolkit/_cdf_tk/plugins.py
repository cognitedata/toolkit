from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml


@dataclass
class Plugin:
    name: str
    description: str

    @staticmethod
    def is_enabled(plugin: Plugin) -> bool:
        user_settings = CDFToml.load()
        return user_settings.plugins.get(plugin.name, False)


class Plugins(Enum):
    dump = Plugin("dump_assets", "plugin for Dump command to retrieve Asset resources from CDF")
    graphql = Plugin("graphql", "GraphQL plugin")

    @staticmethod
    def list() -> dict[str, bool]:
        res = {plugin.name: Plugin.is_enabled(plugin.value) for plugin in Plugins}
        return res
