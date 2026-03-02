from pathlib import Path

import pytest
import typer
from _pytest.monkeypatch import MonkeyPatch

from cognite_toolkit._cdf_tk.commands.functions import FunctionsCommand
from cognite_toolkit._cdf_tk.constants import MODULES
from tests.test_unit.utils import MockQuestionary


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
    verbose: bool = False,
) -> Path:
    """Helper: run FunctionsCommand.init() with all args provided (no prompts needed)."""
    organization_dir = tmp_path / "org"
    _make_module(organization_dir, module_name)
    cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)
    cmd.init(
        organization_dir=organization_dir,
        module_name=module_name,
        external_id=external_id,
        name=name,
        verbose=verbose,
    )
    return organization_dir / MODULES / module_name


class TestFunctionsInitCommand:
    # ── Happy path ────────────────────────────────────────────────────────────

    def test_creates_three_files(self, tmp_path: Path) -> None:
        module_path = _run_init(tmp_path)
        assert (module_path / "functions" / "my-func.Function.yaml").exists()
        assert (module_path / "functions" / "my-func" / "handler.py").exists()
        assert (module_path / "functions" / "my-func" / "requirements.txt").exists()

    def test_yaml_content(self, tmp_path: Path) -> None:
        module_path = _run_init(tmp_path, external_id="test-fn", name="Test Fn")
        yaml_text = (module_path / "functions" / "test-fn.Function.yaml").read_text()
        assert "externalId: test-fn" in yaml_text
        assert "name: Test Fn" in yaml_text
        assert "runtime: py311" in yaml_text
        assert "functionPath: ./handler.py" in yaml_text
        assert "tracing-api-key" in yaml_text
        # Secret variable must use underscores (hyphens replaced)
        assert "test_fn_tracing_api_key" in yaml_text

    def test_handler_py_content(self, tmp_path: Path) -> None:
        module_path = _run_init(tmp_path, name="My Service")
        handler_text = (module_path / "functions" / "my-func" / "handler.py").read_text()
        assert 'FunctionApp(title="My Service"' in handler_text
        assert "@app.get" in handler_text
        assert "@app.post" in handler_text
        assert "handle = create_function_service" in handler_text
        assert '__all__ = ["handle"]' in handler_text

    def test_requirements_txt_content(self, tmp_path: Path) -> None:
        module_path = _run_init(tmp_path)
        req_text = (module_path / "functions" / "my-func" / "requirements.txt").read_text()
        assert "cognite-function-apps[tracing]>=0.4.0" in req_text

    # ── Prompts for external_id / name ────────────────────────────────────────

    def test_prompts_for_external_id_and_name(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        organization_dir = tmp_path / "org"
        _make_module(organization_dir)
        cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)

        with MockQuestionary(
            "cognite_toolkit._cdf_tk.commands.functions",
            monkeypatch,
            ["prompted-func", "Prompted Function"],
        ):
            cmd.init(
                organization_dir=organization_dir,
                module_name="my_module",
                external_id=None,
                name=None,
                verbose=False,
            )

        module_path = organization_dir / MODULES / "my_module"
        assert (module_path / "functions" / "prompted-func.Function.yaml").exists()
        assert (module_path / "functions" / "prompted-func" / "handler.py").exists()

    def test_abort_when_external_id_empty(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        organization_dir = tmp_path / "org"
        _make_module(organization_dir)
        cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)

        with pytest.raises(typer.Exit), MockQuestionary(
            "cognite_toolkit._cdf_tk.commands.functions",
            monkeypatch,
            [""],  # empty external_id answer
        ):
            cmd.init(
                organization_dir=organization_dir,
                module_name="my_module",
                external_id=None,
                name=None,
                verbose=False,
            )

    # ── Overwrite guard ───────────────────────────────────────────────────────

    def test_skips_existing_file_when_user_declines(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _run_init(tmp_path)
        yaml_path = module_path / "functions" / "my-func.Function.yaml"
        original_content = yaml_path.read_text()

        # Decline overwrite for the yaml, accept for handler.py and requirements.txt
        organization_dir = tmp_path / "org"
        cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)
        with MockQuestionary(
            "cognite_toolkit._cdf_tk.commands.functions",
            monkeypatch,
            [False, True, True],  # yaml: skip; handler.py: overwrite; requirements.txt: overwrite
        ):
            cmd.init(
                organization_dir=organization_dir,
                module_name="my_module",
                external_id="my-func",
                name="New Name",
                verbose=False,
            )

        # YAML was not overwritten
        assert yaml_path.read_text() == original_content

    def test_overwrites_existing_file_when_user_confirms(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        module_path = _run_init(tmp_path)
        yaml_path = module_path / "functions" / "my-func.Function.yaml"

        organization_dir = tmp_path / "org"
        cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)
        with MockQuestionary(
            "cognite_toolkit._cdf_tk.commands.functions",
            monkeypatch,
            [True, True, True],  # overwrite all three
        ):
            cmd.init(
                organization_dir=organization_dir,
                module_name="my_module",
                external_id="my-func",
                name="Updated Name",
                verbose=False,
            )

        assert "name: Updated Name" in yaml_path.read_text()

    # ── Module resolution ─────────────────────────────────────────────────────

    def test_creates_new_module_when_not_found(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        organization_dir = tmp_path / "org"
        organization_dir.mkdir(parents=True, exist_ok=True)
        cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)

        # MockQuestionary answers: confirm create new module=True, then resources.py questionary done
        with MockQuestionary(
            [
                "cognite_toolkit._cdf_tk.commands.resources",
                "cognite_toolkit._cdf_tk.commands.functions",
            ],
            monkeypatch,
            [True],  # "module not found — create new?" → yes
        ):
            cmd.init(
                organization_dir=organization_dir,
                module_name="brand_new_module",
                external_id="my-func",
                name="My Function",
                verbose=False,
            )

        new_module = organization_dir / MODULES / "brand_new_module"
        assert (new_module / "functions" / "my-func.Function.yaml").exists()

    def test_abort_when_module_not_found_and_user_declines(self, tmp_path: Path, monkeypatch: MonkeyPatch) -> None:
        organization_dir = tmp_path / "org"
        organization_dir.mkdir(parents=True, exist_ok=True)
        cmd = FunctionsCommand(print_warning=False, skip_tracking=True, silent=True)

        with pytest.raises(typer.Exit), MockQuestionary(
            "cognite_toolkit._cdf_tk.commands.resources",
            monkeypatch,
            [False],  # "module not found — create new?" → no
        ):
            cmd.init(
                organization_dir=organization_dir,
                module_name="nonexistent",
                external_id="my-func",
                name="My Function",
                verbose=False,
            )
