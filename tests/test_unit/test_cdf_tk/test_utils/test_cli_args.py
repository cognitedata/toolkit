import pytest

from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.cli_args import parse_data_model_str, parse_view_str


class TestParseViewStr:
    def test_valid_str(self) -> None:
        view_str = "my_space:MyView/v2"
        view_id = parse_view_str(view_str)
        assert view_id.space == "my_space"
        assert view_id.external_id == "MyView"
        assert view_id.version == "v2"

    @pytest.mark.parametrize(
        "invalid_str, expected_message",
        [
            (
                "invalidformat",
                "Invalid view string format: 'invalidformat'. Expected format 'space:externalId/version'.",
            ),
            ("spaceonly:", "Invalid view string format: 'spaceonly:'. Expected format 'space:externalId/version'."),
            (":/v1", "Invalid view string format: ':/v1'. Expected format 'space:externalId/version'."),
            (
                "space:externalId",
                "Invalid view string format: 'space:externalId'. Expected format 'space:externalId/version'.",
            ),
            (
                "space:externalId/",
                "Invalid view string format: 'space:externalId/'. Expected format 'space:externalId/version'.",
            ),
        ],
    )
    def test_invalid_str(self, invalid_str: str, expected_message: str) -> None:
        with pytest.raises(ToolkitValueError) as exc_info:
            parse_view_str(invalid_str)
        assert str(exc_info.value) == expected_message


class TestParseDataModelStr:
    def test_valid_str(self) -> None:
        data_model_id = parse_data_model_str("my_space:MyModel/v2")
        assert data_model_id.space == "my_space"
        assert data_model_id.external_id == "MyModel"
        assert data_model_id.version == "v2"

    @pytest.mark.parametrize(
        "invalid_str, expected_message",
        [
            (
                "invalidformat",
                "Invalid data model string format: 'invalidformat'. Expected format 'space:externalId/version'.",
            ),
            (
                "spaceonly:",
                "Invalid data model string format: 'spaceonly:'. Expected format 'space:externalId/version'.",
            ),
            (":/v1", "Invalid data model string format: ':/v1'. Expected format 'space:externalId/version'."),
            (
                "space:externalId",
                "Invalid data model string format: 'space:externalId'. Expected format 'space:externalId/version'.",
            ),
            (
                "space:externalId/",
                "Invalid data model string format: 'space:externalId/'. Expected format 'space:externalId/version'.",
            ),
        ],
    )
    def test_invalid_str(self, invalid_str: str, expected_message: str) -> None:
        with pytest.raises(ToolkitValueError) as exc_info:
            parse_data_model_str(invalid_str)
        assert str(exc_info.value) == expected_message
