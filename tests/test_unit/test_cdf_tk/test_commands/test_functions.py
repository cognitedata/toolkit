from pathlib import Path

from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.commands.functions import FunctionsCommand, Route
from cognite_toolkit._cdf_tk.constants import MODULES
from tests.test_unit.utils import MockQuestionary

_DEFAULT_ROUTES = [Route("/process", "Example POST route", True)]


def _make_module(base: Path, module_name: str = "my_module") -> Path:
    """Create a minimal recognised module (needs at least one resource sub-dir)."""
    module_dir = base / MODULES / module_name
    (module_dir / "data_sets").mkdir(parents=True, exist_ok=True)
    return module_dir


def _run_init(
    tmp_path: Path,
    *,
    module_name: str = "my_module",
    external_id: str = "my-func",
    name: str = "My Function",
    routes: list[Route] | None = None,
) -> Path:
    """Helper: run FunctionsCommand.init() with all args provided (no prompts needed)."""
    module_path = _make_module(tmp_path / "org", module_name)
    cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)
    cmd.init(
        module_path=module_path,
        external_id=external_id,
        name=name,
        routes=routes if routes is not None else _DEFAULT_ROUTES,
    )
    return module_path


class TestFunctionsInitCommand:
    # ── Happy path ────────────────────────────────────────────────────────────

    def test_creates_handler_and_requirements(self, tmp_path: Path) -> None:
        module_path = _run_init(tmp_path)
        assert (module_path / "functions" / "my-func" / "handler.py").exists()
        assert (module_path / "functions" / "my-func" / "requirements.txt").exists()

    def test_does_not_create_yaml(self, tmp_path: Path) -> None:
        module_path = _run_init(tmp_path)
        assert not (module_path / "functions" / "my-func.Function.yaml").exists()
        assert not (module_path / "functions" / "my-func.FunctionApp.yaml").exists()

    def test_handler_py_content(self, tmp_path: Path) -> None:
        routes = [
            Route("/process", "Process an asset", True),
            Route("/status", "Health check", False),
        ]
        module_path = _run_init(tmp_path, name="My Service", routes=routes)
        handler_text = (module_path / "functions" / "my-func" / "handler.py").read_text()
        assert 'FunctionApp(title="My Service"' in handler_text
        assert '@app.post("/process")' in handler_text
        assert '@app.post("/status")' in handler_text
        assert "class ProcessRequest(BaseModel):" in handler_text
        # /status has no body, so no StatusRequest model
        assert "StatusRequest" not in handler_text
        assert "handle = create_function_service" in handler_text
        assert '__all__ = ["handle"]' in handler_text

    def test_handler_py_route_docstrings(self, tmp_path: Path) -> None:
        routes = [Route("/do-work", "Does the work", False)]
        module_path = _run_init(tmp_path, routes=routes)
        handler_text = (module_path / "functions" / "my-func" / "handler.py").read_text()
        assert "Does the work" in handler_text
        assert "def do_work(" in handler_text

    def test_handler_py_path_to_func_name(self, tmp_path: Path) -> None:
        routes = [Route("/some-route/action", "Multi-segment route", False)]
        module_path = _run_init(tmp_path, routes=routes)
        handler_text = (module_path / "functions" / "my-func" / "handler.py").read_text()
        assert "def some_route_action(" in handler_text

    def test_requirements_txt_content(self, tmp_path: Path) -> None:
        module_path = _run_init(tmp_path)
        req_text = (module_path / "functions" / "my-func" / "requirements.txt").read_text()
        assert "cognite-function-apps[tracing]>=0.4.0" in req_text

    # ── Prompts for name / routes ─────────────────────────────────────────────

    def test_prompts_for_name_and_routes(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _make_module(tmp_path / "org")
        cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)

        # Answers: name, route path, route desc, has_body=True, add_another=False
        with MockQuestionary(
            "cognite_toolkit._cdf_tk.commands.functions",
            monkeypatch,
            ["Prompted Function", "/process", "My route", True, False],
        ):
            cmd.init(module_path=module_path, external_id="prompted-func")

        assert (module_path / "functions" / "prompted-func" / "handler.py").exists()

    # ── Overwrite guard ───────────────────────────────────────────────────────

    def test_skips_existing_file_when_user_declines(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _run_init(tmp_path)
        handler_path = module_path / "functions" / "my-func" / "handler.py"
        original_content = handler_path.read_text()

        cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)
        with MockQuestionary(
            "cognite_toolkit._cdf_tk.commands.functions",
            monkeypatch,
            [False, True],  # handler.py: skip; requirements.txt: overwrite
        ):
            cmd.init(
                module_path=module_path,
                external_id="my-func",
                name="New Name",
                routes=_DEFAULT_ROUTES,
            )

        # handler.py was not overwritten
        assert handler_path.read_text() == original_content

    def test_overwrites_existing_file_when_user_confirms(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _run_init(tmp_path)
        handler_path = module_path / "functions" / "my-func" / "handler.py"

        cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)
        with MockQuestionary(
            "cognite_toolkit._cdf_tk.commands.functions",
            monkeypatch,
            [True, True],  # overwrite handler.py and requirements.txt
        ):
            cmd.init(
                module_path=module_path,
                external_id="my-func",
                name="Updated Name",
                routes=_DEFAULT_ROUTES,
            )

        assert 'FunctionApp(title="Updated Name"' in handler_path.read_text()
