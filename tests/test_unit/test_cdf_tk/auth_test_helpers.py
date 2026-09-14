import socket
import threading
import time
from collections.abc import Callable
from urllib.parse import parse_qs, urlparse

import httpx


def bind_ephemeral_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def complete_oauth_callback(port: int, auth_url: str) -> None:
    time.sleep(0.2)
    state = parse_qs(urlparse(auth_url).query)["state"][0]
    httpx.get(
        f"http://127.0.0.1:{port}/",
        params={"state": state, "code": "auth-code"},
        timeout=10.0,
    )


def browser_opener(port: int, *, succeeds: bool) -> Callable[[str], bool]:
    def open_auth_url(url: str) -> bool:
        threading.Thread(target=complete_oauth_callback, args=(port, url), daemon=True).start()
        return succeeds

    return open_auth_url
