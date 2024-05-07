from __future__ import annotations

import itertools
from collections import UserList
from collections.abc import Collection
from typing import Generic, TypeVar

from cognite_toolkit._cdf_tk.validation.warning import DataSetMissingWarning, SnakeCaseWarning, TemplateVariableWarning
from cognite_toolkit._cdf_tk.validation.warning.fileread import FileReadWarning

T_Warning = TypeVar("T_Warning", bound=FileReadWarning)


class Warnings(UserList, Generic[T_Warning]):
    def __init__(self, collection: Collection[T_Warning] | None = None):
        super().__init__(collection or [])


class SnakeCaseWarningList(Warnings[SnakeCaseWarning]):
    def __str__(self) -> str:
        output = [""]
        for (file, identifier, id_name), file_warnings in itertools.groupby(
            sorted(self), key=lambda w: (w.filepath, w.id_value, w.id_name)
        ):
            output.append(f"    In File {str(file)!r}")
            output.append(f"    In entry {id_name}={identifier!r}")
            for warning in file_warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)


class TemplateVariableWarningList(Warnings[TemplateVariableWarning]):
    def __str__(self) -> str:
        output = [""]
        for path, module_warnings in itertools.groupby(sorted(self), key=lambda w: w.path):
            if path:
                output.append(f"    In Section {str(path)!r}")
            for warning in module_warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)


class DataSetMissingWarningList(Warnings[DataSetMissingWarning]):
    def __str__(self) -> str:
        output = [""]
        for filepath, warnings in itertools.groupby(sorted(self), key=lambda w: w.filepath):
            output.append(f"    In file {str(filepath)!r}")
            for warning in warnings:
                output.append(f"{'    ' * 2}{warning!s}")

        return "\n".join(output)
