"""Run Function Apps locally."""

import importlib.util
import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from rich import print

if TYPE_CHECKING:
    from cognite_toolkit._cdf_tk.client import ToolkitClient

from ._base import ToolkitCommand


class RunFunctionAppCommand(ToolkitCommand):
    _HANDLER_PATH_ENV_VAR = "CDF_TK_FUNCTION_APP_HANDLER_PATH"

    def run_function_app(
        self,
        path: Path,
        port: int = 8000,
        log_level: str = "info",
    ) -> None:
        """Start a local development server for a Function App."""
        try:
            import uvicorn
        except ImportError:
            print(
                "[bold red]Error:[/] Missing dependencies for the run command.\n"
                "Install with: [bold]uv sync --extra run-function-app[/]"
            )
            raise SystemExit(1)

        handler_path = path.resolve()
        self._validate_handler(handler_path)

        self._run_with_reload(uvicorn, handler_path, port, log_level)

    @staticmethod
    def _load_handler(handler_path: Path) -> Any:
        handler_file = handler_path / "handler.py"
        module_name = f"{handler_path.name}.handler"
        spec = importlib.util.spec_from_file_location(module_name, handler_file)
        if spec is None or spec.loader is None:
            raise RuntimeError(f"Failed to load {handler_file}")
        module = importlib.util.module_from_spec(spec)
        sys.modules[module_name] = module
        sys.path.insert(0, str(handler_path.parent))
        sys.path.insert(0, str(handler_path))
        spec.loader.exec_module(module)
        try:
            return module.handle
        except AttributeError as error:
            raise RuntimeError(f"{handler_file} does not define a handle") from error

    @staticmethod
    def _create_reloading_asgi_app() -> Any:
        from cognite_function_apps.devserver import create_asgi_app

        handler_path_value = os.environ.get(RunFunctionAppCommand._HANDLER_PATH_ENV_VAR)
        if handler_path_value is None:
            raise RuntimeError("Function App reload factory must be started through 'cdf dev run function-app'.")
        handler_path = Path(handler_path_value)
        handle = RunFunctionAppCommand._load_handler(handler_path)
        return create_asgi_app(handle, client_factory=RunFunctionAppCommand._create_cognite_client)

    @staticmethod
    def _run_with_reload(uvicorn: Any, handler_path: Path, port: int, log_level: str) -> None:
        previous_handler_path = os.environ.get(RunFunctionAppCommand._HANDLER_PATH_ENV_VAR)
        os.environ[RunFunctionAppCommand._HANDLER_PATH_ENV_VAR] = str(handler_path)
        try:
            uvicorn.run(
                "cognite_toolkit._cdf_tk.commands.run_function_app:RunFunctionAppCommand._create_reloading_asgi_app",
                host="127.0.0.1",
                port=port,
                reload=True,
                reload_dirs=[str(handler_path)],
                log_level=log_level,
                factory=True,
            )
        finally:
            if previous_handler_path is None:
                del os.environ[RunFunctionAppCommand._HANDLER_PATH_ENV_VAR]
            else:
                os.environ[RunFunctionAppCommand._HANDLER_PATH_ENV_VAR] = previous_handler_path

    @staticmethod
    def _validate_handler(handler_path: Path) -> None:
        if not handler_path.is_dir():
            print(f"[bold red]Error:[/] Path is not a directory: {handler_path}")
            raise SystemExit(1)
        if handler_path.name in sys.stdlib_module_names:
            print(f"[bold red]Error:[/] Directory name '{handler_path.name}' shadows a standard library module.")
            raise SystemExit(1)

        handler_file = handler_path / "handler.py"
        if not handler_file.is_file():
            print(f"[bold red]Error:[/] handler.py not found in {handler_path}")
            raise SystemExit(1)
        try:
            source = handler_file.read_text()
        except OSError as error:
            print(f"[bold red]Error:[/] Could not read {handler_file}: {error}")
            raise SystemExit(1) from error
        if "cognite_function_apps" not in source and "create_function_service" not in source:
            print(
                "[bold red]Error:[/] This handler is not a Function App. "
                "Classical functions with [bold]def handle(client, data)[/] are not supported."
            )
            raise SystemExit(1)

    @staticmethod
    def _create_cognite_client() -> "ToolkitClient":
        from cognite_toolkit._cdf_tk.commands.auth import EnvironmentVariables

        return EnvironmentVariables.create_from_environment().get_client(is_strict_validation=False)
