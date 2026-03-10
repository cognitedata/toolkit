"""Tests for the serve command and landing page middleware."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from cognite_toolkit._cdf_tk.commands._landing_page import LandingPageMiddleware
from cognite_toolkit._cdf_tk.commands.serve import ServeFunctionCommand


# ── ServeFunctionCommand._validate_handler_directory ──


class TestValidateHandlerDirectory:
    def test_nonexistent_path(self, tmp_path: Path) -> None:
        with pytest.raises(SystemExit):
            ServeFunctionCommand._validate_handler_directory(tmp_path / "nope")

    def test_file_not_directory(self, tmp_path: Path) -> None:
        f = tmp_path / "handler.py"
        f.write_text("")
        with pytest.raises(SystemExit):
            ServeFunctionCommand._validate_handler_directory(f)

    def test_invalid_module_name(self, tmp_path: Path) -> None:
        d = tmp_path / "not-valid-ident"
        d.mkdir()
        with pytest.raises(SystemExit):
            ServeFunctionCommand._validate_handler_directory(d)

    def test_stdlib_shadow(self, tmp_path: Path) -> None:
        d = tmp_path / "json"
        d.mkdir()
        with pytest.raises(SystemExit):
            ServeFunctionCommand._validate_handler_directory(d)

    def test_valid_directory(self, tmp_path: Path) -> None:
        d = tmp_path / "my_handler"
        d.mkdir()
        # Should not raise
        ServeFunctionCommand._validate_handler_directory(d)


# ── ServeFunctionCommand._patch_cognite_client_factory ──


class TestPatchCogniteClientFactory:
    def test_patches_get_cognite_client_from_env(self) -> None:
        import types

        mock_client = MagicMock()
        mock_env_vars = MagicMock()
        mock_env_vars.get_client.return_value = mock_client

        # Create a real module object so importlib.import_module can find it
        fake_asgi = types.ModuleType("cognite_function_apps.devserver.asgi")
        fake_asgi.get_cognite_client_from_env = lambda: None  # type: ignore[attr-defined]

        # Also need fake parent packages for the import chain
        import sys

        saved_keys = ["cognite_function_apps.devserver.asgi", "cognite_function_apps.devserver"]
        saved = {k: sys.modules.get(k) for k in saved_keys}

        fake_devserver = types.ModuleType("cognite_function_apps.devserver")
        fake_devserver.asgi = fake_asgi  # type: ignore[attr-defined]
        sys.modules["cognite_function_apps.devserver"] = fake_devserver
        sys.modules["cognite_function_apps.devserver.asgi"] = fake_asgi
        try:
            with patch(
                "cognite_toolkit._cdf_tk.utils.auth.EnvironmentVariables.create_from_environment",
                return_value=mock_env_vars,
            ):
                ServeFunctionCommand._patch_cognite_client_factory()

                # Call the patched factory — must be inside the patch context
                # because it calls EnvironmentVariables.create_from_environment()
                result = fake_asgi.get_cognite_client_from_env()  # type: ignore[attr-defined]

            mock_env_vars.get_client.assert_called_with(is_strict_validation=False)
            assert result is mock_client
        finally:
            for k in saved_keys:
                if saved[k] is None:
                    sys.modules.pop(k, None)
                else:
                    sys.modules[k] = saved[k]


# ── LandingPageMiddleware ──

def _run_async(coro):
    """Helper to run async code in tests."""
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


class _ResponseCollector:
    """Collects ASGI send() calls for inspection."""

    def __init__(self) -> None:
        self.messages: list[dict] = []

    async def __call__(self, message: dict) -> None:
        self.messages.append(message)

    @property
    def status(self) -> int:
        return self.messages[0]["status"]

    @property
    def headers_dict(self) -> dict[str, str]:
        return {
            k.decode(): v.decode()
            for k, v in self.messages[0].get("headers", [])
        }

    @property
    def body(self) -> bytes:
        return b"".join(m.get("body", b"") for m in self.messages if m["type"] == "http.response.body")

    @property
    def body_text(self) -> str:
        return self.body.decode()


def _make_scope(path: str = "/", method: str = "GET") -> dict:
    return {
        "type": "http",
        "method": method,
        "path": path,
        "query_string": b"",
        "headers": [],
    }


async def _noop_receive():
    return {"type": "http.request", "body": b"", "more_body": False}


def _make_middleware(inner_app=None):
    if inner_app is None:
        async def inner_app(scope, receive, send):
            await send({"type": "http.response.start", "status": 200, "headers": [], "trailers": False})
            await send({"type": "http.response.body", "body": b"inner", "more_body": False})

    return LandingPageMiddleware(
        inner_app,
        handler_name="my_func",
        handler_path="/path/to/my_func/handler.py",
        cdf_project="test-project",
        cdf_cluster="westeurope-1",
    )


class TestLandingPageMiddleware:
    def test_landing_page_returns_html(self) -> None:
        mw = _make_middleware()
        collector = _ResponseCollector()
        _run_async(mw(_make_scope("/"), _noop_receive, collector))

        assert collector.status == 200
        assert "text/html" in collector.headers_dict["content-type"]
        body = collector.body_text
        assert "my_func" in body
        assert "test-project" in body
        assert "westeurope-1" in body
        assert "/docs" in body
        assert "read/write" in body.lower() or "read AND WRITE" in body or "read/write access" in body.lower()

    def test_status_endpoint_returns_json(self) -> None:
        mw = _make_middleware()
        collector = _ResponseCollector()
        _run_async(mw(_make_scope("/api/status"), _noop_receive, collector))

        assert collector.status == 200
        assert "application/json" in collector.headers_dict["content-type"]
        data = json.loads(collector.body)
        assert data["handler_name"] == "my_func"
        assert data["handler_path"] == "/path/to/my_func/handler.py"
        assert data["cdf_project"] == "test-project"
        assert data["cdf_cluster"] == "westeurope-1"
        assert "last_reload" in data
        assert "uptime_seconds" in data

    def test_passthrough_to_inner_app(self) -> None:
        mw = _make_middleware()
        collector = _ResponseCollector()
        _run_async(mw(_make_scope("/docs"), _noop_receive, collector))

        assert collector.status == 200
        assert collector.body == b"inner"

    def test_passthrough_non_http(self) -> None:
        called = []

        async def inner_app(scope, receive, send):
            called.append(scope["type"])

        mw = _make_middleware(inner_app)
        scope = {"type": "lifespan"}
        _run_async(mw(scope, _noop_receive, lambda msg: asyncio.sleep(0)))

        assert called == ["lifespan"]

    def test_post_request_passes_through(self) -> None:
        mw = _make_middleware()
        collector = _ResponseCollector()
        _run_async(mw(_make_scope("/", method="POST"), _noop_receive, collector))

        # POST / should pass through to inner app, not serve landing page
        assert collector.body == b"inner"

    def test_sse_logs_endpoint_streams(self) -> None:
        """Test that /api/logs starts an SSE stream with correct headers."""
        import logging

        mw = _make_middleware()

        # Add a log entry so the buffer has content
        logger = logging.getLogger("test.serve")
        logger.setLevel(logging.DEBUG)
        logger.info("test log message")

        collector = _ResponseCollector()

        async def run_sse():
            # The SSE endpoint loops forever; we cancel after getting the initial burst
            task = asyncio.ensure_future(mw(_make_scope("/api/logs"), _noop_receive, collector))
            # Give it time to send buffered logs
            await asyncio.sleep(0.2)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        _run_async(run_sse())

        assert collector.messages[0]["status"] == 200
        headers = {k.decode(): v.decode() for k, v in collector.messages[0]["headers"]}
        assert headers["content-type"] == "text/event-stream"
        assert headers["cache-control"] == "no-cache"
