from pathlib import Path

import pytest
from pydantic import BaseModel, Field, field_validator

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.storageio import ModelList


class MyRow(BaseModel):
    id: int
    name: str
    display_name: str | None = Field(None, alias="displayName")

    @field_validator("display_name", mode="before")
    @classmethod
    def validate_display_name(cls, v: str | None) -> str | None:
        if isinstance(v, str) and v.strip() == "":
            return None
        return v


class MyList(ModelList[MyRow]):
    @classmethod
    def _get_base_model_cls(cls) -> type[MyRow]:
        return MyRow


class TestModelList:
    def test_read_csv(self, tmp_path: Path) -> None:
        csv_content = "id,name,displayName\n1,Alice,Aly\n2,Bob,\ninvalid_row\n3,Charlie,\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        my_list = MyList.read_csv_file(csv_file)

        assert len(my_list) == 3
        assert my_list[0] == MyRow(id=1, name="Alice", displayName="Aly")
        assert my_list[1] == MyRow(id=2, name="Bob")
        assert my_list[2] == MyRow(id=3, name="Charlie")
        assert len(my_list.invalid_rows) == 1
        assert 3 in my_list.invalid_rows

    def test_raise_missing_column(self, tmp_path: Path) -> None:
        csv_content = "id\n1\n2\n3\n"
        csv_file = tmp_path / "test_missing_column.csv"
        csv_file.write_text(csv_content)

        with pytest.raises(ToolkitValueError, match="Missing required columns: name"):
            MyList.read_csv_file(csv_file)
