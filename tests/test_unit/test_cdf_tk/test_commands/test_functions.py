from pathlib import Path

import pytest
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.commands.functions import FunctionsCommand, Route
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


def _run_init(
    tmp_path: Path,
    *,
    external_id: str = "my-func",
    name: str = "My Function",
    routes: list[Route] | None = None,
) -> Path:
    module_path = _make_module(tmp_path / "org")
    _cmd().init(
        module_path=module_path,
        external_id=external_id,
        name=name,
        routes=routes if routes is not None else _DEFAULT_ROUTES,
        prompt_tracing=False,
    )
    return module_path


def _handler_text(module_path: Path, external_id: str = "my-func") -> str:
    return (module_path / "functions" / external_id / "handler.py").read_text()


class TestFunctionsInitScaffold:
    def test_creates_expected_files(self, tmp_path: Path) -> None:
        module_path = _run_init(tmp_path)
        func_dir = module_path / "functions" / "my-func"
        assert (func_dir / "handler.py").exists()
        assert (func_dir / "requirements.txt").exists()
        assert "cognite-function-apps[tracing]>=0.9.0" in (func_dir / "requirements.txt").read_text()
        # YAML files are created by ResourcesCommand, not FunctionsCommand
        assert not (module_path / "functions" / "my-func.Function.yaml").exists()
        assert not (module_path / "functions" / "my-func.FunctionApp.yaml").exists()


class TestHandlerGeneration:
    def test_multi_route_handler(self, tmp_path: Path) -> None:
        routes = [
            Route("/process", "Process an asset", True),
            Route("/status", "Health check", False),
        ]
        module_path = _run_init(tmp_path, name="My Service", routes=routes)
        text = _handler_text(module_path)
        assert 'FunctionApp(title="My Service"' in text
        assert '@app.post("/process")' in text
        assert '@app.post("/status")' in text
        assert "class ProcessRequest(BaseModel):" in text
        assert "StatusRequest" not in text  # no body → no model
        assert "handle = create_function_service" in text
        assert '__all__ = ["handle"]' in text

    @pytest.mark.parametrize(
        "path, expected_func, expected_model",
        [
            ("/do-work", "def do_work(", None),
            ("/some-route/action", "def some_route_action(", "SomeRouteActionRequest"),
            ("/process", "def process(", "ProcessRequest"),
        ],
        ids=["hyphen", "multi-segment", "simple"],
    )
    def test_path_to_identifiers(
        self, tmp_path: Path, path: str, expected_func: str, expected_model: str | None
    ) -> None:
        has_body = expected_model is not None
        module_path = _run_init(tmp_path, routes=[Route(path, "desc", has_body)])
        text = _handler_text(module_path)
        assert expected_func in text
        if expected_model:
            assert f"class {expected_model}(BaseModel):" in text

    @pytest.mark.parametrize(
        "name, routes, expected, forbidden",
        [
            ('"My App"', None, 'FunctionApp(title="My App"', '""""'),
            ("My Function", [Route("/do-work", "Does the work", False)], '"""Does the work"""', None),
        ],
        ids=["quoted-name-stripped", "description-as-docstring"],
    )
    def test_sanitized_output(
        self, tmp_path: Path, name: str, routes: list[Route] | None, expected: str, forbidden: str | None
    ) -> None:
        module_path = _run_init(tmp_path, name=name, routes=routes)
        text = _handler_text(module_path)
        assert expected in text
        if forbidden:
            assert forbidden not in text


class TestInteractivePrompts:
    def test_prompts_for_name_and_routes_no_tracing(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _make_module(tmp_path / "org")
        # Answers: name, route path, desc, has_body, add_another, tracing backend (no tracing)
        with MockQuestionary(_MOCK_TARGET, monkeypatch, ["Prompted Function", "/process", "My route", True, False, ""]):
            _cmd().init(module_path=module_path, external_id="prompted-func")
        text = _handler_text(module_path, "prompted-func")
        assert "create_tracing_app" not in text
        assert "@tracing.trace()" not in text

    def test_quoted_description_is_stripped(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _make_module(tmp_path / "org")
        with MockQuestionary(
            _MOCK_TARGET, monkeypatch, ["My Func", "/hello", '"Answer hello world"', False, False, ""]
        ):
            _cmd().init(module_path=module_path, external_id="quoted-func")
        text = _handler_text(module_path, "quoted-func")
        assert '"""Answer hello world"""' in text
        assert '""""' not in text

    def test_tracing_backend_in_handler(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _make_module(tmp_path / "org")
        # Answers: name, route path, desc, has_body, add_another, tracing backend
        with MockQuestionary(_MOCK_TARGET, monkeypatch, ["My Func", "/hello", "Hello", False, False, "lightstep"]):
            _cmd().init(module_path=module_path, external_id="ls-func")
        text = _handler_text(module_path, "ls-func")
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
        module_path = _run_init(tmp_path)
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
