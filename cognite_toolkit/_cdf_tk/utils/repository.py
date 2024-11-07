import fnmatch
from collections.abc import Iterable
from functools import lru_cache
from pathlib import Path

import requests
from rich.progress import track


class GitHubFileDownloader:
    api_url = "https://api.github.com"

    def __init__(self, repo: str) -> None:
        self.repo = repo

    def copy(self, source: Path, destination: Path) -> None:
        to_download = list(self._find_files(source))
        for path, url in track(to_download, description="Downloading files"):
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

    @lru_cache(maxsize=10)
    def _get_repo_contents(self, path: str = "") -> list[dict[str, str]]:
        url = f"{self.api_url}/repos/{self.repo}/contents/{path}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @staticmethod
    def _download_file(url: str, source: Path, destination: Path) -> None:
        response = requests.get(url)
        response.raise_for_status()
        if destination.is_file():
            destination_path = destination
        elif destination.is_dir():
            destination_path = destination / source.name
        else:
            raise ValueError(f"Destination {destination} is not a file or directory")
        destination_path.parent.mkdir(parents=True, exist_ok=True)
        destination_path.write_bytes(response.content)
