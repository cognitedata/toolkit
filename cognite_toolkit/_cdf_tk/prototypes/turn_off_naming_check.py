from collections.abc import Hashable
from pathlib import Path

from cognite_toolkit._cdf_tk.loaders import RESOURCE_LOADER_LIST, GroupLoader
from cognite_toolkit._cdf_tk.tk_warnings import WarningList, YAMLFileWarning


def no_op(cls: type, identifier: Hashable, filepath: Path, verbose: bool) -> WarningList[YAMLFileWarning]:
    return WarningList[YAMLFileWarning]()


def do() -> None:
    for loader in [*RESOURCE_LOADER_LIST, GroupLoader]:
        loader.check_identifier_semantics = classmethod(no_op)  # type: ignore[attr-defined]
