from functools import lru_cache

from cognite_toolkit._cdf_tk.feature_flags import FeatureFlag, Flags


def enable_all_flags() -> None:
    """Turns on every alpha flag regardless of cdf.toml.

    Resource classes are filtered by flags when ``resource_ios`` is imported, so this must run before that import.
    """

    @lru_cache(typed=True)
    def _always_enabled(flag: Flags) -> bool:
        return True

    FeatureFlag.is_enabled = staticmethod(_always_enabled)  # type: ignore[method-assign, assignment]
