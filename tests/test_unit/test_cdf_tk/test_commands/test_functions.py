from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.commands.functions import FunctionsCommand, Route, _path_to_func_name, _sanitize_route_path
from cognite_toolkit._cdf_tk.constants import MODULES
from tests.test_unit.utils import MockQuestionary

_DEFAULT_ROUTES = [Route("/process", "Example POST route", True)]
_MOCK_TARGET = "cognite_toolkit._cdf_tk.commands.functions"


def _make_module(base: Path) -> Path:
    module_dir = base / MODULES / "my_module"
    (module_dir / "data_sets").mkdir(parents=True, exist_ok=True)
    return module_dir


def _cmd() -> FunctionsCommand:
    return FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)


def _init_and_read(
    tmp_path: Path,
    *,
    external_id: str = "my-func",
    name: str = "My Function",
    routes: list[Route] | None = None,
) -> tuple[Path, str]:
    """Run init and return (module_path, handler.py text)."""
    module_path = _make_module(tmp_path / "org")
    _cmd().init(
        module_path=module_path,
        external_id=external_id,
        name=name,
        routes=routes if routes is not None else _DEFAULT_ROUTES,
        prompt_tracing=False,
    )
    text = (module_path / "functions" / external_id / "handler.py").read_text()
    return module_path, text


class TestSanitizeRoutePath:
    @pytest.mark.parametrize(
        "raw, expected",
        [
            ("/process", "/process"),
            ("process", "/process"),
            ("/My Route", "/my-route"),
            ("/path.to/thing", "/path-to/thing"),
            ("/Under_Score", "/under-score"),
            ("/UPPER/CASE", "/upper/case"),
            ("/a--b//c", "/a-b/c"),
            ("/special!@#chars", "/specialchars"),
            ("  /padded  ", "/padded"),
        ],
        ids=["noop", "no-slash", "spaces", "dots", "underscores", "uppercase", "collapse", "strip-special", "trim"],
    )
    def test_sanitize_route_path(self, raw: str, expected: str) -> None:
        assert _sanitize_route_path(raw) == expected


class TestPathToFuncName:
    @pytest.mark.parametrize(
        "path, expected",
        [
            ("/some-route/action", "some_route_action"),
            ("/do-work", "do_work"),
            ("/process", "process"),
            ("/path.to/thing", "path_to_thing"),
            ("/has spaces", "has_spaces"),
        ],
        ids=["multi-segment", "hyphen", "simple", "dots", "spaces"],
    )
    def test_path_to_func_name(self, path: str, expected: str) -> None:
        assert _path_to_func_name(path) == expected


class TestHandlerGeneration:
    def test_creates_files_and_handler_content(self, tmp_path: Path) -> None:
        routes = [
            Route("/process", "Process an asset", True),
            Route("/status", "Health check", False),
        ]
        module_path, text = _init_and_read(tmp_path, name="My Service", routes=routes)
        func_dir = module_path / "functions" / "my-func"

        # Files created
        assert (func_dir / "handler.py").exists()
        assert "cognite-function-apps[tracing]" in (func_dir / "requirements.txt").read_text()

        # Handler structure
        assert 'FunctionApp(title="My Service"' in text
        assert '@app.post("/process")' in text
        assert '@app.post("/status")' in text
        assert "class ProcessRequest(BaseModel):" in text
        assert "StatusRequest" not in text  # no body → no model
        assert "handle = create_function_service" in text
        assert '__all__ = ["handle"]' in text

    def test_quoted_name_stripped(self, tmp_path: Path) -> None:
        _, text = _init_and_read(tmp_path, name='"My App"')
        assert 'FunctionApp(title="My App"' in text
        assert '""""' not in text

    def test_description_becomes_docstring(self, tmp_path: Path) -> None:
        _, text = _init_and_read(tmp_path, routes=[Route("/do-work", "Does the work", False)])
        assert '"""Does the work"""' in text


class TestInteractivePrompts:
    def test_no_tracing(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _make_module(tmp_path / "org")
        # Answers: name, route path, desc, has_body, add_another, tracing backend
        with MockQuestionary(_MOCK_TARGET, monkeypatch, ["Prompted Function", "/process", "My route", True, False, ""]):
            _cmd().init(module_path=module_path, external_id="prompted-func")
        text = (module_path / "functions" / "prompted-func" / "handler.py").read_text()
        assert "create_tracing_app" not in text

    def test_tracing_backend_selection(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _make_module(tmp_path / "org")
        with MockQuestionary(_MOCK_TARGET, monkeypatch, ["My Func", "/hello", "Hello", False, False, "lightstep"]):
            _cmd().init(module_path=module_path, external_id="ls-func")
        text = (module_path / "functions" / "ls-func" / "handler.py").read_text()
        assert 'backend="lightstep"' in text
        assert "@tracing.trace()" in text


class TestOverwriteGuard:
    @pytest.mark.parametrize(
        "confirm_overwrite, expect_overwritten",
        [(False, False), (True, True)],
        ids=["decline", "confirm"],
    )
    def test_overwrite_behavior(
        self, tmp_path: Path, monkeypatch: MonkeyPatch, confirm_overwrite: bool, expect_overwritten: bool
    ) -> None:
        module_path, _ = _init_and_read(tmp_path)
        handler_path = module_path / "functions" / "my-func" / "handler.py"
        original_content = handler_path.read_text()

        with MockQuestionary(_MOCK_TARGET, monkeypatch, [confirm_overwrite, True]):
            _cmd().init(
                module_path=module_path,
                external_id="my-func",
                name="Updated Name",
                routes=_DEFAULT_ROUTES,
                prompt_tracing=False,
            )

        if expect_overwritten:
            assert 'FunctionApp(title="Updated Name"' in handler_path.read_text()
        else:
            assert handler_path.read_text() == original_content
