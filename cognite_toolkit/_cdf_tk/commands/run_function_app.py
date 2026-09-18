"""Run Function Apps locally."""

import importlib.util
import shutil
import sys
import tempfile
from pathlib import Path
from typing import Any

from rich import print

from ._base import ToolkitCommand


class RunFunctionAppCommand(ToolkitCommand):
    _LOOPBACK_HOSTS = frozenset({"127.0.0.1", "::1", "localhost"})

    def run_function_app(
        self,
        path: Path,
        host: str = "127.0.0.1",
        port: int = 8000,
        reload: bool = True,
        log_level: str = "info",
    ) -> None:
        """Start a local development server for a Function App."""
        try:
            import uvicorn
            from cognite_function_apps.devserver import create_asgi_app
        except ImportError:
            print(
                "[bold red]Error:[/] Missing dependencies for the run command.\n"
                "Install with: [bold]uv sync --extra run-function-app[/]"
            )
            raise SystemExit(1)

        handler_path = path.resolve()
        self._validate_handler_directory(handler_path)
        self._validate_function_app_handler(handler_path)
        self._warn_if_not_loopback(host)

        if reload:
            self._run_with_reload(uvicorn, handler_path, host, port, log_level)
        else:
            self._run_without_reload(uvicorn, create_asgi_app, handler_path, host, port, log_level)

    @staticmethod
    def _run_without_reload(
        uvicorn: Any, create_asgi_app: Any, handler_path: Path, host: str, port: int, log_level: str
    ) -> None:
        original_path = sys.path.copy()
        try:
            handle = RunFunctionAppCommand._load_handler(handler_path)
            uvicorn.run(
                create_asgi_app(handle, client_factory=RunFunctionAppCommand._create_cognite_client),
                host=host,
                port=port,
                log_level=log_level,
            )
        finally:
            sys.path[:] = original_path

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
    def _run_with_reload(uvicorn: Any, handler_path: Path, host: str, port: int, log_level: str) -> None:
        temp_dir = Path(tempfile.mkdtemp(prefix="cdf_run_function_app_"))
        module_path = temp_dir / "_cdf_run_function_app_asgi.py"
        temp_dir_str = str(temp_dir)
        try:
            module_path.write_text(
                "import importlib\n"
                "import sys\n"
                f"sys.path.insert(0, {str(handler_path.parent)!r})\n"
                f"sys.path.insert(0, {str(handler_path)!r})\n"
                "from cognite_function_apps.devserver import create_asgi_app\n"
                "from cognite_toolkit._cdf_tk.commands.run_function_app import RunFunctionAppCommand\n"
                f"handle = importlib.import_module({handler_path.name!r} + '.handler').handle\n"
                "app = create_asgi_app(handle, client_factory=RunFunctionAppCommand._create_cognite_client)\n"
            )
            sys.path.insert(0, temp_dir_str)
            uvicorn.run(
                "_cdf_run_function_app_asgi:app",
                host=host,
                port=port,
                reload=True,
                reload_dirs=[str(handler_path)],
                log_level=log_level,
            )
        finally:
            if temp_dir_str in sys.path:
                sys.path.remove(temp_dir_str)
            shutil.rmtree(temp_dir)

    @staticmethod
    def _validate_handler_directory(handler_path: Path) -> None:
        if not handler_path.is_dir():
            print(f"[bold red]Error:[/] Path is not a directory: {handler_path}")
            raise SystemExit(1)
        if handler_path.name in sys.stdlib_module_names:
            print(f"[bold red]Error:[/] Directory name '{handler_path.name}' shadows a standard library module.")
            raise SystemExit(1)

    @staticmethod
    def _validate_function_app_handler(handler_path: Path) -> None:
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
    def _warn_if_not_loopback(host: str) -> None:
        if host not in RunFunctionAppCommand._LOOPBACK_HOSTS:
            print(
                f"[bold yellow]Warning:[/] Binding to {host} exposes this server to your local network.\n"
                "It runs your handler code using your authenticated CDF credentials and has no "
                "authentication of its own."
            )

    @staticmethod
    def _create_cognite_client() -> object:
        from cognite_toolkit._cdf_tk.commands.auth import EnvironmentVariables

        return EnvironmentVariables.create_from_environment().get_client(is_strict_validation=False)
