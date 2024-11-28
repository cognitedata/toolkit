import fnmatch
from abc import ABC, abstractmethod
from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path
from typing import ClassVar, Literal
from urllib.parse import urljoin

import requests
from requests import Response
from rich import print
from rich.progress import track

from cognite_toolkit._cdf_tk.constants import IN_BROWSER
from cognite_toolkit._cdf_tk.tk_warnings import HTTPWarning


class FileDownloader(ABC):
    _type: ClassVar[str] = "generic"

    @abstractmethod
    def __init__(self, repo: str, errors: Literal["continue", "raise"] = "continue") -> None:
        pass

    @abstractmethod
    def copy(self, source: str, destination: Path) -> None:
        pass


class GitHubFileDownloader(FileDownloader):
    _type: ClassVar[str] = "github"

    api_url = "https://api.github.com"

    def __init__(self, repo: str, errors: Literal["continue", "raise"] = "continue") -> None:
        self.repo = repo
        self.errors = errors

    def copy(self, source: str, destination: Path) -> None:
        source_path = Path(source)
        to_download = list(self._find_files(source_path))
        if IN_BROWSER:
            iterable = to_download
        else:
            iterable = track(to_download, description=f"Downloading from {source_path.as_posix()!r}")  # type: ignore [assignment]
        for path, url in iterable:
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


class HttpFileDownloader(FileDownloader):
    _type: ClassVar[str] = "http"

    def __init__(self, repo: str, errors: Literal["continue", "raise"] = "raise") -> None:
        self.repo = repo
        self.errors = errors

    def copy(self, source: str, destination: Path) -> None:
        if not self.repo.endswith("/"):
            self.repo += "/"

        sources = source.split(";") if ";" in source else [source]
        for source in sources:
            file_url = urljoin(self.repo, source)

            response = requests.get(file_url)
            if response.status_code >= 400:
                if self.errors == "raise":
                    response.raise_for_status()
                print(HTTPWarning("GET", response.text, response.status_code).get_message())
                continue

            location = destination if not destination.is_dir() else destination / Path(source).name
            location.write_bytes(response.content)
