from collections.abc import Hashable
from pathlib import Path

from cognite_toolkit._cdf_tk.loaders import (
    RESOURCE_LOADER_LIST,
)
from cognite_toolkit._cdf_tk.tk_warnings import WarningList, YAMLFileWarning


def do() -> None:
    def no_op(cls: type, identifier: Hashable, filepath: Path, verbose: bool) -> WarningList[YAMLFileWarning]:
        return WarningList[YAMLFileWarning]()

    for loader in RESOURCE_LOADER_LIST:
        loader.check_identifier_semantics = classmethod(no_op)  # type: ignore[method-assign, assignment, arg-type]
