import sys
from abc import ABC, abstractmethod
from collections import UserList
from collections.abc import Collection
from pathlib import Path
from typing import Literal, TypeVar

from cognite.client.utils._text import to_camel_case
from pydantic import BaseModel

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.tk_warnings.fileread import ResourceFormatWarning
from cognite_toolkit._cdf_tk.utils import humanize_collection
from cognite_toolkit._cdf_tk.utils.fileio import CSVReader, SchemaColumn
from cognite_toolkit._cdf_tk.validation import T_BaseModel, instantiate_class

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self


class ModelList(UserList[T_BaseModel], ABC):
    def __init__(
        self,
        collection: Collection[T_BaseModel] | None = None,
        invalid_rows: dict[int, ResourceFormatWarning] | None = None,
    ) -> None:
        super().__init__(collection or [])
        self.invalid_rows = invalid_rows or {}

    @classmethod
    @abstractmethod
    def _get_base_model_cls(cls) -> type[T_BaseModel]:
        raise NotImplementedError()

    @classmethod
    def _required_header_names(cls) -> set[str]:
        model_cls = cls._get_base_model_cls()
        return {field_.alias or field_id for field_id, field_ in model_cls.model_fields.items() if field_.is_required()}

    @classmethod
    def read_csv_file(cls, filepath: Path) -> "Self":
        # We only need to read one row to get the header
        schema = CSVReader.sniff_schema(filepath, sniff_rows=1)
        cls._validate_schema(schema)
        reader = CSVReader(input_file=filepath)
        items: list[T_BaseModel] = []
        invalid_rows: dict[int, ResourceFormatWarning] = {}
        model_cls = cls._get_base_model_cls()
        for row_no, row in enumerate(reader.read_chunks_unprocessed(), start=1):
            result = instantiate_class(row, model_cls, filepath)
            if isinstance(result, model_cls):
                items.append(result)
            elif isinstance(result, ResourceFormatWarning):
                invalid_rows[row_no] = result
            else:
                raise TypeError(f"Unexpected result type: {type(result)}")

        return cls(items, invalid_rows)

    @classmethod
    def _validate_schema(cls, schema: list[SchemaColumn]) -> None:
        actual = {col.name for col in schema}
        if missing_columns := cls._required_header_names() - actual:
            raise ToolkitValueError(f"Missing required columns: {humanize_collection(missing_columns)}")


T_ModelList = TypeVar("T_ModelList", bound=ModelList)


class InstanceIdRow(BaseModel, alias_generator=to_camel_case):
    space: str
    external_id: str
    instance_type: Literal["node", "edge"] = "node"


class InstanceIdCSVList(ModelList[InstanceIdRow]):
    @classmethod
    def _get_base_model_cls(cls) -> type[InstanceIdRow]:
        return InstanceIdRow
