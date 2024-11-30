from __future__ import annotations

from cognite_toolkit._cdf_tk.utils import CDFToolConfig

from ._base import ToolkitCommand


class PurgeCommand(ToolkitCommand):
    def space(
        self,
        ToolGlobals: CDFToolConfig,
        space: str | None = None,
        include_space: bool = False,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Purge a space and all its content"""
        ...

    def dataset(
        self,
        ToolGlobals: CDFToolConfig,
        external_id: str | None = None,
        include_dataset: bool = False,
        dry_run: bool = False,
        verbose: bool = False,
    ) -> None:
        """Purge a dataset and all its content"""
        ...
