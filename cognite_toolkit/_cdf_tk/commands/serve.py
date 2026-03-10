"""Serve command for running a local development server for Function Apps."""

import re
import shutil
import sys
import tempfile
from pathlib import Path

from rich import print

from ._base import ToolkitCommand


class ServeFunctionCommand(ToolkitCommand):
    def serve(
        self,
        path: Path,
        host: str = "127.0.0.1",
        port: int = 8000,
        reload: bool = True,
        log_level: str = "info",
    ) -> None:
        """Start a local development server for testing a function app handler."""
        try:
            import uvicorn  # noqa: PLC0415
        except ImportError:
            print(
                "[bold red]Error:[/] Missing dependencies for serve command.\n"
                "Install with: [bold]pip install cognite-toolkit\\[serve][/]\n"
                "Or with uv: [bold]uv pip install cognite-toolkit\\[serve][/]"
            )
            raise SystemExit(1)

        from cognite_function_apps.cli import _load_handler_from_path  # noqa: PLC0415
        from cognite_function_apps.devserver import create_asgi_app  # noqa: PLC0415

        handler_path = path.resolve()
        self._validate_handler_directory(handler_path)

        print(f"\n[bold green]Starting server at http://{host}:{port}[/]")
        if reload:
            print("[yellow]Auto-reload enabled - watching for changes...[/]")
        print("[yellow]Press CTRL+C to quit[/]\n")

        if reload:
            self._run_server_with_reload(uvicorn, handler_path, host, port, log_level)
        else:
            self._run_server_without_reload(
                uvicorn, _load_handler_from_path, create_asgi_app, handler_path, host, port, log_level
            )

    @staticmethod
    def _validate_handler_directory(handler_path: Path) -> None:
        """Validate that the handler directory exists and is valid."""
        if not handler_path.exists():
            print(f"[bold red]Error:[/] Path does not exist: {handler_path}")
            raise SystemExit(1)

        if not handler_path.is_dir():
            print(f"[bold red]Error:[/] Path is not a directory: {handler_path}")
            raise SystemExit(1)

        dir_name = handler_path.name
        if not dir_name.isidentifier():
            suggested_name = re.sub(r"\W|^(?=\d)", "_", dir_name)
            print(
                f"[bold red]Error:[/] Directory name '{dir_name}' is not a valid Python module name.\n"
                f"[yellow]Suggested name:[/] [green]{suggested_name}[/]"
            )
            raise SystemExit(1)

        if dir_name in sys.stdlib_module_names:
            print(
                f"[bold red]Error:[/] Directory name '{dir_name}' shadows a standard library module.\n"
                "[yellow]Please rename the directory to avoid import conflicts.[/]"
            )
            raise SystemExit(1)

    @staticmethod
    def _run_server_with_reload(
        uvicorn: object, handler_path: Path, host: str, port: int, log_level: str
    ) -> None:
        """Run the development server with auto-reload enabled."""
        package_root = handler_path.parent
        package_name = re.sub(r"\W|^(?=\d)", "_", handler_path.name)

        temp_dir = tempfile.mkdtemp(prefix="cdf_serve_")
        temp_app_file = Path(temp_dir) / "_cdf_serve_asgi_app.py"
        temp_app_file.write_text(
            f'''"""Temporary ASGI app for cdf dev function serve with reload support."""
import sys
from pathlib import Path
import importlib

package_root = Path({str(package_root)!r})
if str(package_root) not in sys.path:
    sys.path.insert(0, str(package_root))

from cognite_function_apps.devserver import create_asgi_app

handler_module = importlib.import_module("{package_name}.handler")
app = create_asgi_app(handler_module.handle)
'''
        )

        temp_dir_added = temp_dir not in sys.path
        if temp_dir_added:
            sys.path.insert(0, temp_dir)

        try:
            uvicorn.run(  # type: ignore[union-attr]
                "_cdf_serve_asgi_app:app",
                host=host,
                port=port,
                reload=True,
                reload_dirs=[str(handler_path)],
                log_level=log_level,
            )
        finally:
            if temp_dir_added and temp_dir in sys.path:
                sys.path.remove(temp_dir)
            shutil.rmtree(temp_dir, ignore_errors=True)

    @staticmethod
    def _run_server_without_reload(
        uvicorn: object,
        _load_handler_from_path: object,
        create_asgi_app: object,
        handler_path: Path,
        host: str,
        port: int,
        log_level: str,
    ) -> None:
        """Run the development server without auto-reload."""
        package_root = str(handler_path.parent)
        package_root_added = package_root not in sys.path
        if package_root_added:
            sys.path.insert(0, package_root)

        try:
            print(f"[blue]Loading handler from {handler_path}/handler.py...[/]")
            handle = _load_handler_from_path(handler_path)  # type: ignore[operator]
            print("[green]Handler loaded successfully[/]")

            print("[blue]Creating ASGI app...[/]")
            asgi_app = create_asgi_app(handle)  # type: ignore[operator]
            print("[green]ASGI app created[/]")

            uvicorn.run(  # type: ignore[union-attr]
                asgi_app,
                host=host,
                port=port,
                reload=False,
                log_level=log_level,
            )
        finally:
            if package_root_added and package_root in sys.path:
                sys.path.remove(package_root)
