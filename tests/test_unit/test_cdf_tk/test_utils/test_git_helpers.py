import json
import shutil
import subprocess
from collections.abc import Iterator
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.utils import git_helpers
from cognite_toolkit._cdf_tk.utils.git_helpers import Git

# Environment variables used to detect the CI/CD environment (see utils/cicd.py) and by the Git helpers.
CI_ENV_VARS = [
    "CI",
    "GITHUB_ACTIONS",
    "GITHUB_HEAD_REF",
    "GITHUB_REF_NAME",
    "GITHUB_EVENT_PATH",
    "GITHUB_EVENT_NAME",
    "GITHUB_TRIGGERING_ACTOR",
    "GITHUB_ACTOR",
    "GITLAB_CI",
    "CI_MERGE_REQUEST_SOURCE_BRANCH_NAME",
    "CI_COMMIT_BRANCH",
    "CI_COMMIT_REF_NAME",
    "CI_DEFAULT_BRANCH",
    "CI_COMMIT_BEFORE_SHA",
    "GITLAB_USER_LOGIN",
    "GITLAB_USER_NAME",
    "TF_BUILD",
    "SYSTEM_PULLREQUEST_SOURCEBRANCH",
    "BUILD_SOURCEBRANCH",
    "BUILD_REQUESTEDFOR",
    "BUILD_REQUESTEDFOREMAIL",
    "BITBUCKET_BUILD_NUMBER",
    "BITBUCKET_BRANCH",
    "BITBUCKET_STEP_TRIGGERER_UUID",
    "CIRCLECI",
    "CIRCLE_BRANCH",
    "CIRCLE_USERNAME",
    "TRAVIS",
    "TRAVIS_PULL_REQUEST_BRANCH",
    "TRAVIS_BRANCH",
    "BUILD_ID",
    "GIT_SSH_COMMAND",
]

CI_MARKERS: dict[str, dict[str, str]] = {
    "github": {"CI": "true", "GITHUB_ACTIONS": "true"},
    "gitlab": {"GITLAB_CI": "true"},
    "azure": {"TF_BUILD": "True"},
    "bitbucket": {"CI": "true", "BITBUCKET_BUILD_NUMBER": "1"},
    "circleci": {"CIRCLECI": "true"},
    "travis": {"TRAVIS": "true"},
}

requires_git = pytest.mark.skipif(shutil.which("git") is None, reason="git is not installed")


@pytest.fixture(autouse=True)
def no_ci_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Make the tests independent of the CI/CD environment they are running in."""
    for name in CI_ENV_VARS:
        monkeypatch.delenv(name, raising=False)


def _set_ci(monkeypatch: pytest.MonkeyPatch, ci: str, env: dict[str, str]) -> None:
    for name, value in {**CI_MARKERS[ci], **env}.items():
        monkeypatch.setenv(name, value)


def _git(cwd: Path, *args: str) -> str:
    return subprocess.run(["git", *args], cwd=cwd, check=True, capture_output=True, text=True).stdout.strip()


def _commit(repo: Path, filename: str, content: str = "content") -> None:
    path = repo / filename
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    _git(repo, "add", filename)
    _git(repo, "commit", "-m", f"Add {filename}")


@pytest.fixture
def isolated_git_config(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """Ignore the global/system git config of the machine running the tests."""
    global_config = tmp_path / "gitconfig"
    global_config.write_text("")
    monkeypatch.setenv("GIT_CONFIG_GLOBAL", str(global_config))
    monkeypatch.setenv("GIT_CONFIG_NOSYSTEM", "1")
    for name in ["GIT_AUTHOR_NAME", "GIT_COMMITTER_NAME"]:
        monkeypatch.setenv(name, "Test Author")
    for name in ["GIT_AUTHOR_EMAIL", "GIT_COMMITTER_EMAIL"]:
        monkeypatch.setenv(name, "test@example.com")


@pytest.fixture
def repo(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, isolated_git_config: None) -> Iterator[Path]:
    """A git repository with a 'main' branch with two commits, checked out as the current working directory."""
    repo = tmp_path / "repo"
    repo.mkdir()
    _git(repo, "init", "--initial-branch=main")
    _commit(repo, "README.md")
    _commit(repo, "modules/a/resource.yaml")
    monkeypatch.chdir(repo)
    yield repo


class TestGitRun:
    """Git must never prompt for credentials, as that would hang a CI/CD pipeline. This cannot be observed
    without a remote requiring authentication, so the environment passed to git is checked directly."""

    def test_run_sets_non_interactive_environment(self, monkeypatch: pytest.MonkeyPatch) -> None:
        run = MagicMock(return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""))
        monkeypatch.setattr(git_helpers.subprocess, "run", run)

        Git.get_uncommitted_changes()

        env = run.call_args.kwargs["env"]
        assert env["GIT_TERMINAL_PROMPT"] == "0"
        assert env["GIT_SSH_COMMAND"] == "ssh -o BatchMode=yes"

    def test_run_respects_user_ssh_command(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("GIT_SSH_COMMAND", "ssh -i my_key")
        run = MagicMock(return_value=subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr=""))
        monkeypatch.setattr(git_helpers.subprocess, "run", run)

        Git.get_uncommitted_changes()

        assert run.call_args.kwargs["env"]["GIT_SSH_COMMAND"] == "ssh -i my_key"

    def test_git_not_installed(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("PATH", "")

        assert Git.is_git_repo() is False
        with pytest.raises(RuntimeError, match="Git is not installed"):
            Git.get_uncommitted_changes()


class TestCIEnvironment:
    @pytest.mark.parametrize(
        "ref, expected",
        [
            pytest.param("refs/heads/feature/x", "feature/x", id="Head ref"),
            pytest.param("refs/remotes/origin/main", "main", id="Remote ref"),
            pytest.param("refs/tags/v1.0", "v1.0", id="Tag ref"),
            pytest.param("origin/main", "main", id="Remote branch"),
            pytest.param("main", "main", id="Plain branch"),
            pytest.param("feature/origin/x", "feature/origin/x", id="Origin not prefix"),
        ],
    )
    def test_strip_ref_prefix(self, ref: str, expected: str) -> None:
        assert Git._strip_ref_prefix(ref) == expected

    @pytest.mark.parametrize(
        "ci, env, expected",
        [
            pytest.param("github", {"GITHUB_REF_NAME": "main"}, "main", id="GitHub push"),
            pytest.param(
                "github", {"GITHUB_HEAD_REF": "feature/x", "GITHUB_REF_NAME": "42/merge"}, "feature/x", id="GitHub PR"
            ),
            pytest.param("gitlab", {"CI_COMMIT_BRANCH": "feature/x"}, "feature/x", id="GitLab push"),
            pytest.param(
                "gitlab",
                {"CI_MERGE_REQUEST_SOURCE_BRANCH_NAME": "feature/x", "CI_COMMIT_REF_NAME": "other"},
                "feature/x",
                id="GitLab MR",
            ),
            pytest.param("gitlab", {"CI_COMMIT_REF_NAME": "v1.0"}, "v1.0", id="GitLab tag"),
            pytest.param("azure", {"BUILD_SOURCEBRANCH": "refs/heads/feature/x"}, "feature/x", id="Azure push"),
            pytest.param(
                "azure",
                {"SYSTEM_PULLREQUEST_SOURCEBRANCH": "refs/heads/feature/x", "BUILD_SOURCEBRANCH": "refs/pull/1/merge"},
                "feature/x",
                id="Azure PR",
            ),
            pytest.param("azure", {"BUILD_SOURCEBRANCH": "refs/tags/v1.0"}, "v1.0", id="Azure tag"),
            pytest.param("bitbucket", {"BITBUCKET_BRANCH": "feature/x"}, "feature/x", id="Bitbucket"),
            pytest.param("circleci", {"CIRCLE_BRANCH": "feature/x"}, "feature/x", id="CircleCI"),
            pytest.param(
                "travis",
                {"TRAVIS_PULL_REQUEST_BRANCH": "feature/x", "TRAVIS_BRANCH": "main"},
                "feature/x",
                id="Travis PR",
            ),
            pytest.param("travis", {"TRAVIS_PULL_REQUEST_BRANCH": " ", "TRAVIS_BRANCH": "main"}, "main", id="Blank"),
            pytest.param("github", {}, None, id="No branch variables"),
        ],
    )
    def test_ci_current_branch(
        self, ci: str, env: dict[str, str], expected: str | None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_ci(monkeypatch, ci, env)

        assert Git._ci_current_branch() == expected

    def test_ci_current_branch_local(self) -> None:
        assert Git._ci_current_branch() is None

    @pytest.mark.parametrize(
        "event, expected",
        [
            pytest.param({"repository": {"default_branch": "develop"}}, "develop", id="Default branch"),
            pytest.param({"repository": {}}, None, id="Missing default branch"),
            pytest.param({}, None, id="Missing repository"),
            pytest.param(["not", "a", "dict"], None, id="Invalid event"),
        ],
    )
    def test_ci_default_branch_github(
        self, event: object, expected: str | None, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        event_path = tmp_path / "event.json"
        event_path.write_text(json.dumps(event))
        _set_ci(monkeypatch, "github", {"GITHUB_EVENT_PATH": str(event_path)})

        assert Git._ci_default_branch() == expected

    def test_ci_default_branch_github_missing_event_file(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_ci(monkeypatch, "github", {"GITHUB_EVENT_PATH": str(tmp_path / "does-not-exist.json")})

        assert Git._ci_default_branch() is None

    @pytest.mark.parametrize(
        "ci, env, expected",
        [
            pytest.param("gitlab", {"CI_DEFAULT_BRANCH": "develop"}, "develop", id="GitLab"),
            pytest.param("azure", {"BUILD_SOURCEBRANCH": "refs/heads/main"}, None, id="Azure not exposed"),
            pytest.param("bitbucket", {"BITBUCKET_BRANCH": "main"}, None, id="Bitbucket not exposed"),
        ],
    )
    def test_ci_default_branch(
        self, ci: str, env: dict[str, str], expected: str | None, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        _set_ci(monkeypatch, ci, env)

        assert Git._ci_default_branch() == expected

    @pytest.mark.parametrize(
        "event_name, before, expected",
        [
            pytest.param("push", "abc123", "abc123", id="Push"),
            pytest.param("push", "0" * 40, None, id="New branch push"),
            pytest.param("pull_request", "abc123", None, id="Pull request is ignored"),
            pytest.param("push", None, None, id="Missing before"),
        ],
    )
    def test_ci_before_sha_github(
        self,
        event_name: str,
        before: str | None,
        expected: str | None,
        tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        event_path = tmp_path / "event.json"
        event_path.write_text(json.dumps({"before": before} if before else {}))
        _set_ci(monkeypatch, "github", {"GITHUB_EVENT_PATH": str(event_path), "GITHUB_EVENT_NAME": event_name})

        assert Git._ci_before_sha() == expected

    @pytest.mark.parametrize(
        "before, expected",
        [
            pytest.param("abc123", "abc123", id="Push"),
            pytest.param("0" * 40, None, id="Merge request pipeline"),
        ],
    )
    def test_ci_before_sha_gitlab(self, before: str, expected: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_ci(monkeypatch, "gitlab", {"CI_COMMIT_BEFORE_SHA": before})

        assert Git._ci_before_sha() == expected

    @pytest.mark.parametrize(
        "ci, env, expected",
        [
            pytest.param(
                "github", {"GITHUB_TRIGGERING_ACTOR": "alice", "GITHUB_ACTOR": "bob"}, "alice", id="GitHub triggering"
            ),
            pytest.param("github", {"GITHUB_ACTOR": "bob"}, "bob", id="GitHub actor"),
            pytest.param("gitlab", {"GITLAB_USER_LOGIN": "alice"}, "alice", id="GitLab"),
            pytest.param("azure", {"BUILD_REQUESTEDFOREMAIL": "alice@example.com"}, "alice@example.com", id="Azure"),
            pytest.param("bitbucket", {"BITBUCKET_STEP_TRIGGERER_UUID": "{uuid}"}, "{uuid}", id="Bitbucket"),
            pytest.param("circleci", {"CIRCLE_USERNAME": "alice"}, "alice", id="CircleCI"),
            pytest.param("travis", {}, None, id="Travis not supported"),
        ],
    )
    def test_ci_user(self, ci: str, env: dict[str, str], expected: str | None, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_ci(monkeypatch, ci, env)

        assert Git._ci_user() == expected


@requires_git
class TestGitWithRepository:
    def test_is_git_repo(self, repo: Path) -> None:
        assert Git.is_git_repo() is True

    def test_is_not_git_repo(self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        not_a_repo = tmp_path / "not_a_repo"
        not_a_repo.mkdir()
        monkeypatch.chdir(not_a_repo)
        monkeypatch.setenv("GIT_CEILING_DIRECTORIES", str(tmp_path))

        assert Git.is_git_repo() is False

    def test_get_current_branch(self, repo: Path) -> None:
        _git(repo, "checkout", "-b", "feature/x")

        assert Git.get_current_branch() == "feature/x"

    def test_get_current_branch_local_branch_wins_over_ci(self, repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_ci(monkeypatch, "github", {"GITHUB_REF_NAME": "other"})

        assert Git.get_current_branch() == "main"

    def test_get_current_branch_detached_head_in_ci(self, repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _git(repo, "checkout", "--detach")
        _set_ci(monkeypatch, "github", {"GITHUB_REF_NAME": "feature/x"})

        assert Git.get_current_branch() == "feature/x"

    def test_get_current_branch_detached_head_locally(self, repo: Path) -> None:
        _git(repo, "checkout", "--detach")

        assert Git.get_current_branch() == "HEAD"

    @pytest.mark.parametrize("branch", ["main", "master"])
    def test_get_default_branch_local_fallback(self, branch: str, repo: Path) -> None:
        _git(repo, "branch", "-m", branch)

        assert Git.get_default_branch() == branch

    def test_get_default_branch_unknown_falls_back_to_main(self, repo: Path) -> None:
        _git(repo, "branch", "-m", "trunk")

        assert Git.get_default_branch() == "main"

    def test_get_default_branch_from_ci(self, repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_ci(monkeypatch, "gitlab", {"CI_DEFAULT_BRANCH": "develop"})

        assert Git.get_default_branch() == "develop"

    def test_get_default_branch_from_remote(self, repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _git(repo, "branch", "-m", "develop")
        clone = tmp_path / "clone"
        _git(tmp_path, "clone", "--quiet", str(repo), str(clone))
        # Simulate a CI clone where origin/HEAD is not set.
        _git(clone, "remote", "set-head", "origin", "--delete")
        monkeypatch.chdir(clone)

        assert Git.get_default_branch() == "develop"

    def test_get_changed_files_three_dot(self, repo: Path) -> None:
        _git(repo, "checkout", "-b", "feature")
        _commit(repo, "modules/b/resource.yaml")
        _git(repo, "checkout", "main")
        _commit(repo, "modules/c/resource.yaml")  # Change on main after branching, must not be included.

        assert Git.get_changed_files("main", "feature") == ["modules/b/resource.yaml"]

    def test_get_changed_files_detached_head_in_ci(self, repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _git(repo, "checkout", "-b", "feature")
        _commit(repo, "modules/b/resource.yaml")
        _git(repo, "checkout", "--detach")
        _git(repo, "branch", "-D", "feature")  # In CI, the local branch typically does not exist.
        _set_ci(monkeypatch, "github", {"GITHUB_REF_NAME": "feature"})

        assert Git.get_changed_files("main", "feature") == ["modules/b/resource.yaml"]

    def test_get_changed_files_shallow_clone(self, repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _git(repo, "checkout", "-b", "feature")
        _commit(repo, "modules/b/resource.yaml")
        _commit(repo, "modules/b/other.yaml")
        _git(repo, "checkout", "main")
        _commit(repo, "modules/c/resource.yaml")
        clone = tmp_path / "clone"
        # file:// is required for --depth to take effect for a local clone.
        _git(tmp_path, "clone", "--quiet", "--depth=1", "--branch=feature", repo.as_uri(), str(clone))
        monkeypatch.chdir(clone)
        assert Git._is_shallow() is True

        assert sorted(Git.get_changed_files("main", "feature")) == ["modules/b/other.yaml", "modules/b/resource.yaml"]

    def test_get_changed_files_unrelated_histories_warns(self, repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _git(repo, "checkout", "--orphan", "other")
        _git(repo, "rm", "-r", "--cached", "--quiet", ".")
        # Different content, otherwise git reports a rename of modules/a/resource.yaml.
        _commit(repo, "modules/b/resource.yaml", content="other content")
        print_warning = MagicMock()
        monkeypatch.setattr(git_helpers.MediumSeverityWarning, "print_warning", print_warning)

        changed = Git.get_changed_files("main", "other")

        # No common ancestor, so the tips are compared and files only on 'main' are included as well.
        assert sorted(changed) == ["README.md", "modules/a/resource.yaml", "modules/b/resource.yaml"]
        print_warning.assert_called_once()

    def test_get_changed_files_last_commit(self, repo: Path) -> None:
        _commit(repo, "modules/b/resource.yaml")

        assert Git.get_changed_files_last_commit() == ["modules/b/resource.yaml"]

    @pytest.mark.usefixtures("isolated_git_config")
    def test_get_changed_files_last_commit_initial_commit(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        repo = tmp_path / "new_repo"
        repo.mkdir()
        _git(repo, "init", "--initial-branch=main")
        _commit(repo, "README.md")
        monkeypatch.chdir(repo)

        assert Git.get_changed_files_last_commit() == ["README.md"]

    def test_get_changed_files_last_commit_uses_push_before_sha(
        self, repo: Path, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        before = _git(repo, "rev-parse", "HEAD")
        _commit(repo, "modules/b/resource.yaml")
        _commit(repo, "modules/c/resource.yaml")
        event_path = tmp_path / "event.json"
        event_path.write_text(json.dumps({"before": before}))
        _set_ci(monkeypatch, "github", {"GITHUB_EVENT_PATH": str(event_path), "GITHUB_EVENT_NAME": "push"})

        assert sorted(Git.get_changed_files_last_commit()) == ["modules/b/resource.yaml", "modules/c/resource.yaml"]

    def test_get_uncommitted_changes(self, repo: Path) -> None:
        (repo / "README.md").write_text("changed")

        assert Git.get_uncommitted_changes() == ["README.md"]

    def test_get_me_from_config(self, repo: Path) -> None:
        _git(repo, "config", "user.name", "Alice")

        assert Git.get_me() == "Alice"

    def test_get_me_from_ci(self, repo: Path, monkeypatch: pytest.MonkeyPatch) -> None:
        _set_ci(monkeypatch, "github", {"GITHUB_ACTOR": "alice-gh"})

        assert Git.get_me() == "alice-gh"

    def test_get_me_not_configured(self, repo: Path) -> None:
        with pytest.raises(RuntimeError, match=r"user.name"):
            Git.get_me()
