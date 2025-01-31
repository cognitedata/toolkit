from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import clean_name


@dataclass
class Plugin:
    name: str
    description: str

    def is_enabled(self) -> bool:
        return CDFToml.load().plugins.get(clean_name(self.name), False)


class Plugins(Enum):
    run = Plugin("run", "plugin for Run command to execute Python scripts in CDF")
    dump = Plugin("dump", "plugin for Dump command to retrieve Asset resources from CDF")
    purge = Plugin("purge", "plugin for Purge command to remove datasets and spaces from CDF")

    @staticmethod
    def list() -> dict[str, bool]:
        res = {plugin.name: Plugin.is_enabled(plugin.value) for plugin in Plugins}
        return res
