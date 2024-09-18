from __future__ import annotations

from enum import Enum
from functools import lru_cache

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml


class Plugins(Enum):
    NULL = (0,)
    GRAPHQL = (1,)


class Plugin:
    @staticmethod
    @lru_cache(typed=True)
    def is_enabled(plugin: Plugins) -> bool:
        return CDFToml.load().plugins.get(str(plugin), False)
