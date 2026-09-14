import pytest

from cognite_toolkit._cdf_tk.apps._migrate_app import _validate_cdf_project
from cognite_toolkit._cdf_tk.exceptions import ToolkitValidationError
from tests.test_unit.utils import MockQuestionary


class TestValidateCdfProject:
    @pytest.mark.parametrize(
        "cli_cdf_project, answers",
        [
            pytest.param("my-project", [], id="matching CLI argument"),
            pytest.param(None, ["my-project"], id="matching typed project"),
        ],
    )
    def test_happy_path(self, cli_cdf_project: str | None, answers: list[str], monkeypatch: pytest.MonkeyPatch) -> None:
        with MockQuestionary(_validate_cdf_project.__module__, monkeypatch, answers):
            _validate_cdf_project(cli_cdf_project, "my-project")

    @pytest.mark.parametrize(
        "cli_cdf_project, answers, expected_message",
        [
            pytest.param(
                "other-project",
                [],
                "The CDF project in your command argument does not match your credentials, "
                "'other-project'≠'my-project'.",
                id="mismatched CLI argument",
            ),
            pytest.param(
                None,
                ["wrong"],
                "The CDF project you typed does not match your credentials, 'wrong'≠'my-project'.",
                id="mismatched typed project",
            ),
        ],
    )
    def test_raises(
        self,
        cli_cdf_project: str | None,
        answers: list[str],
        expected_message: str,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with (
            pytest.raises(ToolkitValidationError) as exc_info,
            MockQuestionary(_validate_cdf_project.__module__, monkeypatch, answers),
        ):
            _validate_cdf_project(cli_cdf_project, "my-project")
        assert str(exc_info.value) == expected_message
