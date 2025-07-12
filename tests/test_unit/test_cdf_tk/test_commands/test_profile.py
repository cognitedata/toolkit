import pytest
from rich.spinner import Spinner

from cognite_toolkit._cdf_tk.commands import ProfileRawCommand


class TestProfileCommand:
    @pytest.mark.parametrize(
        "this_row, last_row, expected",
        [
            pytest.param(
                ["MyTable", "13", "2", "-", "-", "-"],
                ["MyTable", "12", "1", "-", "-", "-"],
                ["", "13", "2", "-", "-", "-"],
                id="No change in table name",
            ),
            pytest.param(
                ["MyTable", "13", "2", "-", "-", "-"],
                [],
                ["MyTable", "13", "2", "-", "-", "-"],
                id="Empty last row",
            ),
            pytest.param(
                ["MyTable", "13", "2", "my_transformation", "assets", "upsert"],
                ["OtherTable", "13", "2", "my_transformation", "assets", "upsert"],
                ["MyTable", "13", "2", "my_transformation", "assets", "upsert"],
                id="Change in table name with transformation",
            ),
            pytest.param(
                [
                    "MyTable",
                    Spinner(**ProfileRawCommand.spinner_args),
                    Spinner(**ProfileRawCommand.spinner_args),
                    "-",
                    "-",
                    "-",
                ],
                [
                    "MyTable",
                    Spinner(**ProfileRawCommand.spinner_args),
                    Spinner(**ProfileRawCommand.spinner_args),
                    "-",
                    "-",
                    "-",
                ],
                ["", "", "", "", "", ""],
                id="Spinner in both rows",
            ),
        ],
    )
    def test_create_draw_row(
        self, this_row: list[str | Spinner], last_row: list[str | Spinner], expected: list[str | Spinner]
    ) -> None:
        draw_row = ProfileRawCommand._create_draw_row(this_row, last_row)

        assert draw_row == expected
