"""Creating new class for the SequenceRows and SequenceRowsList to be used in the toolkit. This is because the
SequenceRows and SequenceRowsList classes in the cognite-sdk-python are not compatible with the CDF API.

The new classes includes Writable version with the .as_write() method as well that the write version of the rows
only need a list of strings for the columns and not the entire column object.
"""

import collections.abc
from typing import Any

from cognite.client import CogniteClient
from cognite.client.data_classes._base import (
    CogniteResource,
    CogniteResourceList,
    IdTransformerMixin,
    WriteableCogniteResource,
    WriteableCogniteResourceList,
)
from cognite.client.data_classes.sequences import SequenceColumn, SequenceColumnList, SequenceRow, SequenceRows
from cognite.client.utils._auxiliary import at_least_one_is_not_none


class ToolkitSequenceRowsWrite(CogniteResource):
    """An object representing a list of rows from a sequence.

    Args:
        rows (typing.Sequence[SequenceRow]): The sequence rows.
        columns (collections.abc.Sequence[str): The column information.
        id (int | None): Identifier of the sequence the data belong to
        external_id (str | None): External id of the sequence the data belong to
    """

    def __init__(
        self,
        rows: collections.abc.Sequence[SequenceRow],
        columns: collections.abc.Sequence[str],
        id: int | None = None,
        external_id: str | None = None,
    ) -> None:
        if not at_least_one_is_not_none(id, external_id):
            raise ValueError("At least one of id and external_id must be specified.")

        col_length = len(columns)
        if wrong_length := [r for r in rows if len(r.values) != col_length]:
            raise ValueError(
                f"Rows {[r.row_number for r in wrong_length]} have wrong number of values, expected {col_length}"
            )
        self.rows = rows
        self.columns = columns
        self.id = id
        self.external_id = external_id

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the sequence data into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representing the instance.
        """
        key = "rowNumber" if camel_case else "row_number"
        dumped: dict[str, Any] = {
            "columns": list(self.columns),
            "rows": [{key: r.row_number, "values": r.values} for r in self.rows],
        }
        if self.id is not None:
            dumped["id"] = self.id
        if self.external_id is not None:
            dumped["externalId" if camel_case else "external_id"] = self.external_id
        return dumped

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> "ToolkitSequenceRowsWrite":
        return cls(
            rows=[SequenceRow._load(r) for r in resource["rows"]],
            columns=resource["columns"],
            id=resource.get("id"),
            external_id=resource.get("externalId"),
        )

    def as_sequence_rows(self) -> SequenceRows:
        return SequenceRows(
            self.rows,
            SequenceColumnList([SequenceColumn(external_id=ext_id) for ext_id in self.columns]),
            self.id,
            self.external_id,
        )


class ToolkitSequenceRows(WriteableCogniteResource[ToolkitSequenceRowsWrite]):
    """An object representing a list of rows from a sequence.

    Args:
        rows (typing.Sequence[SequenceRow]): The sequence rows.
        columns (SequenceColumnList): The column information.
        id (int | None): Identifier of the sequence the data belong to
        external_id (str | None): External id of the sequence the data belong to
    """

    def __init__(
        self,
        rows: collections.abc.Sequence[SequenceRow],
        columns: SequenceColumnList,
        id: int,
        external_id: str | None = None,
    ) -> None:
        col_length = len(columns)
        if wrong_length := [r for r in rows if len(r.values) != col_length]:
            raise ValueError(
                f"Rows {[r.row_number for r in wrong_length]} have wrong number of values, expected {col_length}"
            )
        self.rows = rows
        self.columns = columns
        self.id = id
        self.external_id = external_id

    def dump(self, camel_case: bool = True) -> dict[str, Any]:
        """Dump the sequence data into a json serializable Python data type.

        Args:
            camel_case (bool): Use camelCase for attribute names. Defaults to True.

        Returns:
            dict[str, Any]: A dictionary representing the instance.
        """
        key = "rowNumber" if camel_case else "row_number"
        dumped: dict[str, Any] = {
            "columns": self.columns.dump(camel_case),
            "rows": [{key: r.row_number, "values": r.values} for r in self.rows],
        }
        if self.id is not None:
            dumped["id"] = self.id
        if self.external_id is not None:
            dumped["externalId" if camel_case else "external_id"] = self.external_id
        return dumped

    @classmethod
    def _load(cls, resource: dict, cognite_client: CogniteClient | None = None) -> "ToolkitSequenceRows":
        return cls(
            rows=[SequenceRow._load(r) for r in resource["rows"]],
            columns=SequenceColumnList._load(resource["columns"]),
            id=resource["id"],
            external_id=resource.get("externalId"),
        )

    def as_write(self) -> ToolkitSequenceRowsWrite:
        return ToolkitSequenceRowsWrite(self.rows, [c.external_id for c in self.columns], self.id, self.external_id)  # type: ignore[misc]


class ToolkitSequenceRowsWriteList(CogniteResourceList, IdTransformerMixin):
    _RESOURCE = ToolkitSequenceRowsWrite


class ToolkitSequenceRowsList(
    WriteableCogniteResourceList[ToolkitSequenceRowsWrite, ToolkitSequenceRows], IdTransformerMixin
):
    _RESOURCE = ToolkitSequenceRows

    def as_write(self) -> ToolkitSequenceRowsWriteList:
        return ToolkitSequenceRowsWriteList([item.as_write() for item in self])
