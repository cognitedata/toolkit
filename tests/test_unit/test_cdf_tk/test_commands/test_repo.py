from pathlib import Path

from cognite_toolkit._cdf_tk.commands.repo import RepoCommand


class TestRepoCommand:
    @staticmethod
    def _run_repo_init(tmp_path: Path) -> None:
        RepoCommand(skip_git_verify=True, print_warning=False, skip_tracking=True).init(tmp_path, host="None")

    def test_repo_init_appends_missing_gitignore_entries(self, tmp_path: Path) -> None:
        gitignore_path = tmp_path / ".gitignore"
        gitignore_path.write_text("custom-entry/\n*.orig\n", encoding="utf-8")

        self._run_repo_init(tmp_path)

        content = gitignore_path.read_text(encoding="utf-8")
        assert "custom-entry/" in content
        assert "*.orig" in content
        assert content.count("*.orig") == 1
        assert ".DS_Store" in content
        assert RepoCommand._GITIGNORE_MERGE_HEADER in content

    def test_repo_init_does_not_modify_complete_gitignore(self, tmp_path: Path) -> None:
        cmd = RepoCommand(skip_git_verify=True, print_warning=False, skip_tracking=True)
        template_gitignore = (cmd._repo_files / ".gitignore").read_text(encoding="utf-8")
        gitignore_path = tmp_path / ".gitignore"
        gitignore_path.write_text(template_gitignore, encoding="utf-8")

        self._run_repo_init(tmp_path)

        assert gitignore_path.read_text(encoding="utf-8") == template_gitignore
