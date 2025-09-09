from pathlib import Path

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.storageio import ModelList


class MyRow(BaseModel):
    id: int
    name: str


class MyList(ModelList[MyRow]):
    @classmethod
    def _get_base_model_cls(cls) -> type[MyRow]:
        return MyRow


class TestModelList:
    def test_read_csv(self, tmp_path: Path) -> None:
        csv_content = "id,name\n1,Alice\n2,Bob\ninvalid_row\n3,Charlie\n"
        csv_file = tmp_path / "test.csv"
        csv_file.write_text(csv_content)

        my_list = MyList.read_csv_file(csv_file)

        assert len(my_list) == 3
        assert my_list[0] == MyRow(id=1, name="Alice")
        assert my_list[1] == MyRow(id=2, name="Bob")
        assert my_list[2] == MyRow(id=3, name="Charlie")
        assert len(my_list.invalid_rows) == 1
        assert 3 in my_list.invalid_rows
