"""Helpers for interacting with git.

These helpers are designed to work both on a developer machine and in CI/CD environments such as
GitHub Actions, Azure DevOps, GitLab CI, Bitbucket Pipelines, CircleCI, and Travis CI.

Typical CI/CD pitfalls handled here:
* Detached HEAD: CI systems usually check out a specific commit, so `git rev-parse --abbrev-ref HEAD`
  returns 'HEAD'. The branch name is then read from the CI environment variables.
* Shallow clones: Only a few (often one) commits are fetched, and other branches are not available locally.
  Missing refs are looked up under 'origin/' and, if needed, fetched.
* Missing 'origin/HEAD': CI clones rarely set up the remote HEAD symbolic ref, so the default branch is read
  from the CI environment, or queried from the remote.
* Missing git user configuration: the user is read from the CI environment instead.
"""

import json
import os
import subprocess
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.utils.cicd import get_cicd_environment

_NULL_SHA = "0" * 40


class Git:
    @classmethod
    def _run(cls, *args: str, check: bool = True) -> subprocess.CompletedProcess[str]:
        """Run a git command and return the completed process.

        Raises:
            subprocess.CalledProcessError: If the command fails and check is True.
            RuntimeError: If git is not installed.
        """
        env = os.environ.copy()
        # Never prompt for credentials, this would hang in a CI/CD environment.
        env["GIT_TERMINAL_PROMPT"] = "0"
        try:
            return subprocess.run(
                ["git", *args],
                capture_output=True,
                text=True,
                check=check,
                env=env,
            )
        except FileNotFoundError as e:
            raise RuntimeError("Git is not installed or not available on PATH.") from e

    @classmethod
    def _output(cls, *args: str) -> str | None:
        """Run a git command and return the stripped stdout, or None if the command failed."""
        try:
            result = cls._run(*args, check=False)
        except RuntimeError:
            return None
        if result.returncode != 0:
            return None
        return result.stdout.strip()

    @staticmethod
    def _env(*names: str) -> str | None:
        """Return the first non-empty environment variable among the given names."""
        for name in names:
            value = os.environ.get(name, "").strip()
            if value:
                return value
        return None

    @staticmethod
    def _strip_ref_prefix(ref: str) -> str:
        for prefix in ("refs/heads/", "refs/remotes/origin/", "origin/"):
            if ref.startswith(prefix):
                return ref[len(prefix) :]
        return ref

    @classmethod
    def _github_event(cls) -> dict[str, Any]:
        event_path = os.environ.get("GITHUB_EVENT_PATH")
        if not event_path:
            return {}
        try:
            data = json.loads(Path(event_path).read_text(encoding="utf-8"))
        except (OSError, ValueError):
            return {}
        return data if isinstance(data, dict) else {}

    # ---------- CI/CD environment lookups ----------

    @classmethod
    def _ci_current_branch(cls) -> str | None:
        env = get_cicd_environment()
        branch: str | None = None
        if env == "github":
            # GITHUB_HEAD_REF is only set for pull requests and is then the source branch.
            branch = cls._env("GITHUB_HEAD_REF", "GITHUB_REF_NAME")
        elif env == "gitlab":
            branch = cls._env("CI_MERGE_REQUEST_SOURCE_BRANCH_NAME", "CI_COMMIT_BRANCH", "CI_COMMIT_REF_NAME")
        elif env == "azure":
            # BUILD_SOURCEBRANCHNAME only contains the last path segment, so use the full ref instead.
            branch = cls._env("SYSTEM_PULLREQUEST_SOURCEBRANCH", "BUILD_SOURCEBRANCH")
        elif env == "bitbucket":
            branch = cls._env("BITBUCKET_BRANCH")
        elif env == "circleci":
            branch = cls._env("CIRCLE_BRANCH")
        elif env == "travis":
            branch = cls._env("TRAVIS_PULL_REQUEST_BRANCH", "TRAVIS_BRANCH")
        return cls._strip_ref_prefix(branch) if branch else None

    @classmethod
    def _ci_default_branch(cls) -> str | None:
        env = get_cicd_environment()
        if env == "github":
            repository = cls._github_event().get("repository")
            if isinstance(repository, dict):
                default_branch = repository.get("default_branch")
                if isinstance(default_branch, str) and default_branch:
                    return default_branch
        elif env == "gitlab":
            return cls._env("CI_DEFAULT_BRANCH")
        return None

    @classmethod
    def _ci_before_sha(cls) -> str | None:
        """The commit that the branch pointed to before the push that triggered the pipeline (if known)."""
        env = get_cicd_environment()
        sha: str | None = None
        if env == "github":
            before = cls._github_event().get("before")
            sha = before if isinstance(before, str) else None
        elif env == "gitlab":
            sha = cls._env("CI_COMMIT_BEFORE_SHA")
        if not sha or sha == _NULL_SHA:
            return None
        return sha

    @classmethod
    def _ci_user(cls) -> str | None:
        env = get_cicd_environment()
        if env == "github":
            return cls._env("GITHUB_TRIGGERING_ACTOR", "GITHUB_ACTOR")
        elif env == "gitlab":
            return cls._env("GITLAB_USER_LOGIN", "GITLAB_USER_NAME")
        elif env == "azure":
            return cls._env("BUILD_REQUESTEDFOR", "BUILD_REQUESTEDFOREMAIL")
        elif env == "bitbucket":
            return cls._env("BITBUCKET_STEP_TRIGGERER_UUID")
        elif env == "circleci":
            return cls._env("CIRCLE_USERNAME")
        return None

    # ---------- Ref helpers ----------

    @classmethod
    def _ref_exists(cls, ref: str) -> bool:
        return cls._output("rev-parse", "--verify", "--quiet", f"{ref}^{{commit}}") is not None

    @classmethod
    def _is_shallow(cls) -> bool:
        return cls._output("rev-parse", "--is-shallow-repository") == "true"

    @classmethod
    def _resolve_ref(cls, branch: str) -> str:
        """Resolve a branch name to a ref that exists locally.

        In CI/CD environments, branches are often only available as remote-tracking refs, or not fetched at all.
        """
        if branch == "HEAD" or cls._ref_exists(branch):
            return branch
        name = cls._strip_ref_prefix(branch)
        remote_ref = f"origin/{name}"
        if cls._ref_exists(remote_ref):
            return remote_ref
        # The branch currently checked out (detached HEAD in CI).
        if name == cls.get_current_branch():
            return "HEAD"
        # Try to fetch the branch from the remote (typical for shallow clones in CI).
        cls._output("fetch", "--no-tags", "origin", f"+refs/heads/{name}:refs/remotes/origin/{name}")
        if cls._ref_exists(remote_ref):
            return remote_ref
        # Might be a commit SHA or tag, let git decide.
        return branch

    @classmethod
    def _ensure_commit(cls, sha: str) -> bool:
        if cls._ref_exists(sha):
            return True
        cls._output("fetch", "--no-tags", "origin", sha)
        return cls._ref_exists(sha)

    @classmethod
    def _diff_names(cls, *args: str) -> list[str] | None:
        output = cls._output("diff", "--name-only", *args)
        if output is None:
            return None
        return output.splitlines()

    # ---------- Public API ----------

    @classmethod
    def is_git_repo(cls) -> bool:
        """Check if the current directory is a git repository."""
        return cls._output("rev-parse", "--is-inside-work-tree") == "true"

    @classmethod
    def get_default_branch(cls) -> str:
        """Get the default branch of the current git repository."""
        if ci_branch := cls._ci_default_branch():
            return ci_branch

        ref = cls._output("rev-parse", "--abbrev-ref", "origin/HEAD")
        if ref and ref != "origin/HEAD":
            return ref.split("/", 1)[-1] if "/" in ref else ref

        # CI clones rarely have origin/HEAD set, ask the remote directly.
        remote = cls._output("ls-remote", "--symref", "origin", "HEAD")
        if remote:
            for line in remote.splitlines():
                # Format: "ref: refs/heads/main\tHEAD"
                if line.startswith("ref:"):
                    return cls._strip_ref_prefix(line[len("ref:") :].split("\t", 1)[0].strip())

        for branch in ["main", "master"]:
            if cls._output("show-ref", "--verify", f"refs/heads/{branch}") is not None:
                return branch
            if cls._output("show-ref", "--verify", f"refs/remotes/origin/{branch}") is not None:
                return branch
        return "main"

    @classmethod
    def get_current_branch(cls) -> str:
        """Get the current branch of the current git repository.

        In CI/CD environments, the repository is typically checked out in detached HEAD state. In that case,
        the branch name is read from the CI/CD environment variables.
        """
        try:
            result = cls._run("rev-parse", "--abbrev-ref", "HEAD")
        except subprocess.CalledProcessError as e:
            if ci_branch := cls._ci_current_branch():
                return ci_branch
            raise RuntimeError(f"Failed to get current branch: {e.stderr.strip()}") from e
        branch = result.stdout.strip()
        if branch == "HEAD" and (ci_branch := cls._ci_current_branch()):
            return ci_branch
        return branch

    @classmethod
    def get_changed_files(cls, base_branch: str, target_branch: str) -> list[str]:
        """Get the list of files changed on the target branch compared to the base branch."""
        base_ref = cls._resolve_ref(base_branch)
        target_ref = cls._resolve_ref(target_branch)

        # Three-dot: changes on target since it diverged from base. Requires the merge base to be available.
        changed = cls._diff_names(f"{base_ref}...{target_ref}")
        if changed is None and cls._is_shallow():
            # Shallow clone without the merge base, deepen the history and retry.
            cls._output("fetch", "--no-tags", "--deepen=200", "origin")
            changed = cls._diff_names(f"{base_ref}...{target_ref}")
        if changed is not None:
            return changed

        # Fallback: compare the tips of the two refs directly.
        try:
            result = cls._run("diff", "--name-only", base_ref, target_ref)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get changed files: {e.stderr.strip()}") from e
        return result.stdout.strip().splitlines()

    @classmethod
    def get_changed_files_last_commit(cls) -> list[str]:
        """Get the list of changed files in the last commit.

        In a CI/CD environment where the pipeline is triggered by a push, all files changed in the push
        are returned (if the CI/CD system exposes the previous commit). For merge commits, the changes
        are compared against the first parent, i.e., the changes merged into the branch.
        """
        before_sha = cls._ci_before_sha()
        if before_sha and cls._ensure_commit(before_sha):
            changed = cls._diff_names(before_sha, "HEAD")
            if changed is not None:
                return changed

        if not cls._ref_exists("HEAD~1") and cls._is_shallow():
            # Shallow clone (e.g. depth=1), fetch the parent commit.
            cls._output("fetch", "--no-tags", "--deepen=1", "origin")

        if cls._ref_exists("HEAD~1"):
            changed = cls._diff_names("HEAD~1", "HEAD")
            if changed is not None:
                return changed

        # No parent available (initial commit), list all files in the commit.
        try:
            result = cls._run("diff-tree", "--root", "--no-commit-id", "--name-only", "-r", "HEAD")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get changed files in last commit: {e.stderr.strip()}") from e
        return result.stdout.strip().splitlines()

    @classmethod
    def get_uncommitted_changes(cls) -> list[str]:
        """Get the list of uncommitted changes in the current working directory."""
        try:
            result = cls._run("diff", "--name-only")
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Failed to get uncommitted changes: {e.stderr.strip()}") from e
        return result.stdout.strip().splitlines()

    @classmethod
    def get_me(cls) -> str:
        """Get the current git user name. In CI/CD, falls back to the user that triggered the pipeline."""
        name = cls._output("config", "user.name")
        if name:
            return name
        if ci_user := cls._ci_user():
            return ci_user
        raise RuntimeError("Failed to get git user name: 'user.name' is not configured.")
