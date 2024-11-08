import fnmatch
from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path
from typing import Literal

import requests
from requests import Response
from rich import print
from rich.progress import track

from cognite_toolkit._cdf_tk.tk_warnings import HTTPWarning


class GitHubFileDownloader:
    api_url = "https://api.github.com"

    def __init__(self, repo: str, errors: Literal["continue", "raise"] = "continue") -> None:
        self.repo = repo
        self.errors = errors

    def copy(self, source: str, destination: Path) -> None:
        source_path = Path(source)
        to_download = list(self._find_files(source_path))
        for path, url in track(to_download, description=f"Downloading from {source_path.as_posix()!r}"):
            self._download_file(url, path, destination)

    def _find_files(self, source: Path) -> Iterable[tuple[Path, str]]:
        search = [""]
        parents = set(source.parents)
        while search:
            path = search.pop()
            content = self._get_repo_contents(path)
            for item in content:
                repo_path = Path(item["path"])
                if item["type"] == "file" and fnmatch.fnmatch(repo_path.name, source.name):
                    yield repo_path, item["download_url"]
                elif item["type"] == "dir" and repo_path in parents:
                    search.append(item["path"])

    @lru_cache
    def _get_repo_contents(self, path: str = "") -> list[dict[str, str]]:
        url = f"{self.api_url}/repos/{self.repo}/contents/{path}"
        response = self._requests(url, "get repo contents")
        if response is None:
            return []
        return response.json()

    def _requests(self, url: str, action: str) -> Response | None:
        response = requests.get(url)
        if response.status_code >= 400:
            if self.errors == "raise":
                response.raise_for_status()
            print(HTTPWarning(action, response.text, response.status_code).get_message())
            return None
        return response

    def _download_file(self, url: str, source: Path, destination: Path) -> None:
        response = self._requests(url, f"download {source}")
        if response is None:
            return
        if destination.suffix:
            destination_path = destination
        else:
            destination_path = destination / source.name
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        destination_path.write_bytes(response.content)
