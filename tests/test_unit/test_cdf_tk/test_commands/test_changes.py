from pathlib import Path

from cognite_toolkit._cdf_tk.commands import _changes
from cognite_toolkit._cdf_tk.commands._changes import AddCommonDirectoriesToGitignore


class TestAddCommonDirectoriesToGitignore:
    @staticmethod
    def _run(tmp_path: Path) -> set[Path]:
        return AddCommonDirectoriesToGitignore(tmp_path, None).do()

    def test_adds_missing_entries_to_existing_gitignore(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        gitignore_path = tmp_path / ".gitignore"
        gitignore_path.write_text("data/\n", encoding="utf-8")

        changed = self._run(tmp_path)

        assert changed == {gitignore_path}
        content = gitignore_path.read_text(encoding="utf-8")
        assert "tmp/" in content
        assert "logs/" in content

    def test_does_nothing_when_entries_already_present(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)
        gitignore_path = tmp_path / ".gitignore"
        gitignore_path.write_text("data/\ntmp/\nlogs/\n", encoding="utf-8")

        assert self._run(tmp_path) == set()

    def test_does_nothing_when_no_gitignore_exists(self, tmp_path: Path, monkeypatch) -> None:
        monkeypatch.chdir(tmp_path)

        assert self._run(tmp_path) == set()

    def test_updates_gitignore_at_git_root_when_organization_dir_is_nested(
        self, tmp_path: Path, monkeypatch
    ) -> None:
        repo_root = tmp_path
        organization_dir = repo_root / "my_organization"
        organization_dir.mkdir()
        gitignore_path = repo_root / ".gitignore"
        gitignore_path.write_text("data/\n", encoding="utf-8")
        monkeypatch.chdir(organization_dir)
        monkeypatch.setattr(_changes._cli_commands, "use_git", lambda: True)
        monkeypatch.setattr(_changes._cli_commands, "has_initiated_repo", lambda: True)
        monkeypatch.setattr(_changes._cli_commands, "git_root", lambda: repo_root)

        changed = self._run(organization_dir)

        assert changed == {gitignore_path}
        content = gitignore_path.read_text(encoding="utf-8")
        assert "tmp/" in content
        assert "logs/" in content
