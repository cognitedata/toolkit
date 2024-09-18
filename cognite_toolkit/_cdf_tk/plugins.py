from __future__ import annotations

from functools import lru_cache

from cognite_toolkit._cdf_tk.cdf_toml import CDFToml
from cognite_toolkit._cdf_tk.constants import clean_name


class Plugins:
    pass


class PluginDefinition:
    @staticmethod
    @lru_cache(typed=True)
    def is_enabled(name: str) -> bool:
        user_settings = CDFToml.load().feature_flags
        return False if not user_settings.get(clean_name(name), False) else True
