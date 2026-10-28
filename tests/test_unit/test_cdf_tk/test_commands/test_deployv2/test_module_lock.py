from collections.abc import Iterator
from datetime import UTC, datetime, timedelta
from unittest.mock import MagicMock

import pytest

from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import NodeRequest, NodeResponse
from cognite_toolkit._cdf_tk.client.testing import ToolkitClientMock, monkeypatch_toolkit_client
from cognite_toolkit._cdf_tk.commands.deploy_v2.module_lock import (
    EXTERNAL_ID,
    HEARTBEAT_INTERVAL_SECONDS,
    LEGACY_VIEW_IDS,
    MAX_MODULE_COUNT,
    SPACE,
    VERSION,
    VIEW_ID,
    ErrorLockResult,
    FailedLock,
    FailedLockResult,
    ModuleLock,
    ModuleLockManager,
    SuccessfulLockResult,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils.git_helpers import Git
from cognite_toolkit._cdf_tk.utils.user import UserInfo

USER_ID = "user-123"
GIT_NAME = "alice"
FEATURE_BRANCH = "feature/my-change"
DEPLOY_BRANCH = "main"
REPO = "github.com/cognitedata/toolkit"


def _lock_node(
    module_id: str,
    *,
    branch: str = "feature/other",
    lock: bool = True,
    timestamp: datetime | None = None,
    version: int = 3,
    git_name: str = "bob",
    user_id: str = "user-456",
    repo: str | None = None,
) -> NodeResponse:
    timestamp = timestamp or datetime.now(UTC)
    return NodeResponse.model_validate(
        {
            "instanceType": "node",
            "space": SPACE,
            "externalId": module_id,
            "version": version,
            "createdTime": 0,
            "lastUpdatedTime": 0,
            "properties": {
                SPACE: {
                    f"{EXTERNAL_ID}/{VERSION}": {
                        "moduleId": module_id,
                        "timestamp": timestamp.isoformat(),
                        "gitName": git_name,
                        "userId": user_id,
                        "branch": branch,
                        "lock": lock,
                        "repo": repo,
                    }
                }
            },
        }
    )


def _view_missing_error() -> ToolkitAPIError:
    return ToolkitAPIError("One or more views do not exist: cognite_toolkit:ModuleLock/v1", code=400)


@pytest.fixture
def client() -> Iterator[ToolkitClientMock]:
    with monkeypatch_toolkit_client() as client:
        client.lookup.users = MagicMock()
        client.lookup.users.email.return_value = "bob@example.com"
        client.instances.retrieve.return_value = []
        client.instances.create.return_value = []
        yield client


@pytest.fixture
def git() -> MagicMock:
    git = MagicMock(spec=Git)
    git.is_git_repo.return_value = True
    git.get_current_branch.return_value = FEATURE_BRANCH
    git.get_default_branch.return_value = DEPLOY_BRANCH
    git.get_me.return_value = GIT_NAME
    git.get_remote_repo.return_value = REPO
    return git


@pytest.fixture(autouse=True)
def user_info(monkeypatch: pytest.MonkeyPatch) -> MagicMock:
    load = MagicMock(return_value=UserInfo(type="user", id=USER_ID))
    monkeypatch.setattr(UserInfo, "load", load)
    return load


def _created_nodes(client: ToolkitClientMock) -> list[NodeRequest]:
    assert client.instances.create.call_count == 1
    return client.instances.create.call_args.args[0]


class TestModuleLock:
    @pytest.mark.parametrize(
        "age, expected",
        [
            pytest.param(timedelta(seconds=0), False, id="Fresh lock"),
            pytest.param(timedelta(seconds=HEARTBEAT_INTERVAL_SECONDS - 60), False, id="Just before expiry"),
            pytest.param(timedelta(seconds=HEARTBEAT_INTERVAL_SECONDS + 60), True, id="Just after expiry"),
            pytest.param(timedelta(days=30), True, id="Long expired"),
        ],
    )
    def test_is_expired(self, age: timedelta, expected: bool) -> None:
        lock = ModuleLock(
            module_id="my_module",
            timestamp=datetime.now(UTC) - age,
            git_name=GIT_NAME,
            user_id=USER_ID,
            branch=FEATURE_BRANCH,
            lock=True,
            existing_version=None,
        )
        assert lock.is_expired is expected

    def test_from_nodes_parses_properties_and_version(self) -> None:
        timestamp = datetime(2026, 10, 24, 12, 0, tzinfo=UTC)
        node = _lock_node("my_module", branch="feature/x", timestamp=timestamp, version=7)

        locks = ModuleLock.from_nodes([node])

        assert locks == [
            ModuleLock(
                module_id="my_module",
                timestamp=timestamp,
                git_name="bob",
                user_id="user-456",
                branch="feature/x",
                lock=True,
                existing_version=7,
            )
        ]
        # The response properties must not be mutated.
        assert node.properties is not None
        assert "existingVersion" not in node.properties[VIEW_ID]

    def test_from_nodes_skips_nodes_without_lock_properties(self) -> None:
        no_properties = NodeResponse(space=SPACE, external_id="a", version=1, created_time=0, last_updated_time=0)
        other_view = NodeResponse.model_validate(
            {
                "instanceType": "node",
                "space": SPACE,
                "externalId": "b",
                "version": 1,
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "properties": {SPACE: {"OtherView/v1": {"name": "b"}}},
            }
        )

        assert ModuleLock.from_nodes([no_properties, other_view]) == []

    def test_from_nodes_invalid_properties_raises(self) -> None:
        node = NodeResponse.model_validate(
            {
                "instanceType": "node",
                "space": SPACE,
                "externalId": "my_module",
                "version": 1,
                "createdTime": 0,
                "lastUpdatedTime": 0,
                "properties": {SPACE: {f"{EXTERNAL_ID}/{VERSION}": {"moduleId": "my_module"}}},
            }
        )

        with pytest.raises(ToolkitValueError, match="Failed to parse module lock from node my_module"):
            ModuleLock.from_nodes([node])

    def test_as_nodes(self) -> None:
        lock = ModuleLock(
            module_id="my_module",
            timestamp=datetime(2026, 10, 24, 12, 0, tzinfo=UTC),
            git_name=GIT_NAME,
            user_id=USER_ID,
            branch=FEATURE_BRANCH,
            lock=True,
            existing_version=0,
        )

        nodes = ModuleLock.as_nodes([lock])

        assert len(nodes) == 1
        node = nodes[0]
        assert isinstance(node, NodeRequest)
        assert (node.space, node.external_id, node.existing_version) == (SPACE, "my_module", 0)
        assert node.sources is not None and len(node.sources) == 1
        source = node.sources[0]
        assert source.source == VIEW_ID
        assert source.properties is not None
        assert set(source.properties.keys()) == {"moduleId", "timestamp", "gitName", "userId", "branch", "lock", "repo"}
        assert source.properties["branch"] == FEATURE_BRANCH
        assert source.properties["lock"] is True

    def test_as_nodes_legacy_view_excludes_repo(self) -> None:
        lock = ModuleLock(
            module_id="my_module",
            timestamp=datetime(2026, 10, 24, 12, 0, tzinfo=UTC),
            git_name=GIT_NAME,
            user_id=USER_ID,
            branch=FEATURE_BRANCH,
            lock=False,
            existing_version=1,
            repo=REPO,
        )
        node = ModuleLock.as_nodes([lock], LEGACY_VIEW_IDS[0])[0]
        assert node.sources is not None and node.sources[0].source == LEGACY_VIEW_IDS[0]
        assert node.sources[0].properties is not None
        assert "repo" not in node.sources[0].properties

    def test_as_nodes_from_nodes_round_trip(self) -> None:
        lock = ModuleLock(
            module_id="my_module",
            timestamp=datetime(2026, 10, 24, 12, 0, tzinfo=UTC),
            git_name=GIT_NAME,
            user_id=USER_ID,
            branch=FEATURE_BRANCH,
            lock=False,
            existing_version=4,
        )
        request = ModuleLock.as_nodes([lock])[0]
        assert request.sources is not None and request.sources[0].properties is not None
        response = NodeResponse(
            space=request.space,
            external_id=request.external_id,
            version=4,
            created_time=0,
            last_updated_time=0,
            properties={VIEW_ID: request.sources[0].properties},
        )

        assert ModuleLock.from_nodes([response]) == [lock]


class TestModuleLockManagerTryLock:
    @pytest.mark.parametrize(
        "is_git_repo, user_id, module_ids, expected_message",
        [
            pytest.param(
                True, USER_ID, [f"m{i}" for i in range(MAX_MODULE_COUNT + 1)], "Cannot lock more than", id="Too many"
            ),
            pytest.param(False, USER_ID, ["m1"], "Not a git repository", id="Not git repo"),
            pytest.param(True, None, ["m1"], "user ID is None", id="Missing user id"),
        ],
    )
    def test_try_lock_error(
        self,
        is_git_repo: bool,
        user_id: str | None,
        module_ids: list[str],
        expected_message: str,
        client: ToolkitClientMock,
        git: MagicMock,
        user_info: MagicMock,
    ) -> None:
        git.is_git_repo.return_value = is_git_repo
        user_info.return_value = UserInfo(type="unknown", id=user_id)

        result = ModuleLockManager(client, git, dry_run=False).try_lock(module_ids)

        assert isinstance(result, ErrorLockResult)
        assert expected_message in result.error_message
        client.instances.retrieve.assert_not_called()
        client.instances.create.assert_not_called()

    @pytest.mark.parametrize(
        "current_branch, expected_is_deploy_branch",
        [
            pytest.param(DEPLOY_BRANCH, True, id="Deploy branch"),
            pytest.param(FEATURE_BRANCH, False, id="Feature branch"),
        ],
    )
    def test_try_lock_no_modules(
        self, current_branch: str, expected_is_deploy_branch: bool, client: ToolkitClientMock, git: MagicMock
    ) -> None:
        git.get_current_branch.return_value = current_branch

        result = ModuleLockManager(client, git, dry_run=False).try_lock([])

        assert result == SuccessfulLockResult(
            module_ids=[],
            is_deploy_branch=expected_is_deploy_branch,
            user_id=USER_ID,
            git_name=GIT_NAME,
            current_branch=current_branch,
            repo=REPO,
        )
        client.instances.retrieve.assert_not_called()
        client.instances.create.assert_not_called()

    def test_try_lock_on_deploy_branch_does_not_lock(self, client: ToolkitClientMock, git: MagicMock) -> None:
        git.get_current_branch.return_value = DEPLOY_BRANCH

        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1", "m2", "m1"])

        assert result == SuccessfulLockResult(
            module_ids=["m1", "m2"],
            is_deploy_branch=True,
            user_id=USER_ID,
            git_name=GIT_NAME,
            current_branch=DEPLOY_BRANCH,
            repo=REPO,
        )
        client.instances.retrieve.assert_not_called()
        client.instances.create.assert_not_called()

    def test_try_lock_explicit_deploy_branch_overrides_default(self, client: ToolkitClientMock, git: MagicMock) -> None:
        git.get_current_branch.return_value = "release"

        result = ModuleLockManager(client, git, dry_run=False, deploy_branch="release").try_lock(["m1"])

        assert isinstance(result, SuccessfulLockResult)
        assert result.is_deploy_branch is True
        git.get_default_branch.assert_not_called()

    def test_try_lock_new_modules(self, client: ToolkitClientMock, git: MagicMock) -> None:
        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1", "m2", "m1"])

        assert result == SuccessfulLockResult(
            module_ids=["m1", "m2"],
            is_deploy_branch=False,
            user_id=USER_ID,
            git_name=GIT_NAME,
            current_branch=FEATURE_BRANCH,
            repo=REPO,
        )
        nodes = _created_nodes(client)
        assert [node.external_id for node in nodes] == ["m1", "m2"]
        # New nodes use existing_version=0, i.e., the create fails if another process created the lock first.
        assert [node.existing_version for node in nodes] == [0, 0]
        assert client.instances.create.call_args.kwargs == {"replace": True}
        assert nodes[0].sources is not None
        properties = nodes[0].sources[0].properties
        assert properties is not None
        assert properties["lock"] is True
        assert properties["branch"] == FEATURE_BRANCH
        assert properties["userId"] == USER_ID
        assert properties["gitName"] == GIT_NAME
        assert properties["repo"] == REPO

    @pytest.mark.parametrize(
        "existing",
        [
            pytest.param(_lock_node("m1", lock=False, version=5), id="Released lock"),
            pytest.param(
                _lock_node(
                    "m1", timestamp=datetime.now(UTC) - timedelta(seconds=HEARTBEAT_INTERVAL_SECONDS + 1), version=5
                ),
                id="Expired lock",
            ),
            pytest.param(_lock_node("m1", branch=FEATURE_BRANCH, version=5), id="Locked by same branch"),
            pytest.param(
                _lock_node("m1", branch=FEATURE_BRANCH, git_name="someone-else", user_id="other", version=5),
                id="Same branch other user",
            ),
            pytest.param(_lock_node("m1", branch=FEATURE_BRANCH, repo=REPO, version=5), id="Same branch same repo"),
        ],
    )
    def test_try_lock_existing_lock_can_be_taken(
        self, existing: NodeResponse, client: ToolkitClientMock, git: MagicMock
    ) -> None:
        client.instances.retrieve.return_value = [existing]

        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])

        assert isinstance(result, SuccessfulLockResult)
        nodes = _created_nodes(client)
        assert len(nodes) == 1
        # Optimistic concurrency: the version of the existing lock is used.
        assert nodes[0].existing_version == 5

    def test_try_lock_locked_by_other_branch(self, client: ToolkitClientMock, git: MagicMock) -> None:
        client.instances.retrieve.return_value = [
            _lock_node("m1", branch="feature/other", git_name="bob", user_id="user-456"),
            _lock_node("m2", lock=False),
        ]
        client.lookup.users.email.return_value = None

        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1", "m2", "m3"])

        assert result == FailedLockResult(
            failed_locks=[
                FailedLock(module_id="m1", git_name="bob", user_id="user-456", user_email=None, branch="feature/other")
            ],
        )
        client.instances.create.assert_not_called()

    def test_try_lock_same_branch_other_repo_fails(self, client: ToolkitClientMock, git: MagicMock) -> None:
        client.instances.retrieve.return_value = [_lock_node("m1", branch=FEATURE_BRANCH, repo="github.com/other/repo")]
        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])
        assert isinstance(result, FailedLockResult)
        client.instances.create.assert_not_called()

    def test_try_lock_repo_lookup_fails(self, client: ToolkitClientMock, git: MagicMock) -> None:
        git.get_remote_repo.side_effect = RuntimeError("git failed")
        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])
        assert isinstance(result, SuccessfulLockResult)
        assert result.repo is None

    def test_try_lock_dry_run_does_not_write(self, client: ToolkitClientMock, git: MagicMock) -> None:
        result = ModuleLockManager(client, git, dry_run=True).try_lock(["m1"])

        assert isinstance(result, SuccessfulLockResult)
        assert result.is_deploy_branch is False
        client.instances.retrieve.assert_called_once()
        client.instances.create.assert_not_called()

    def test_try_lock_dry_run_still_reports_conflicts(self, client: ToolkitClientMock, git: MagicMock) -> None:
        client.instances.retrieve.return_value = [_lock_node("m1")]

        result = ModuleLockManager(client, git, dry_run=True).try_lock(["m1"])

        assert isinstance(result, FailedLockResult)
        client.instances.create.assert_not_called()

    def test_try_lock_creates_view_when_missing(self, client: ToolkitClientMock, git: MagicMock) -> None:
        client.instances.retrieve.side_effect = [_view_missing_error(), []]

        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])

        assert isinstance(result, SuccessfulLockResult)
        assert client.instances.retrieve.call_count == 2
        client.spaces.create.assert_called_once()
        client.containers.create.assert_called_once()
        client.views.create.assert_called_once()
        view = client.views.create.call_args.args[0][0]
        assert (view.space, view.external_id, view.version) == (SPACE, EXTERNAL_ID, VERSION)
        container = client.containers.create.call_args.args[0][0]
        assert set(view.properties.keys()) == set(container.properties.keys())
        _created_nodes(client)

    def test_try_lock_view_missing_twice_returns_error(self, client: ToolkitClientMock, git: MagicMock) -> None:
        client.instances.retrieve.side_effect = [_view_missing_error(), _view_missing_error()]

        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])

        assert isinstance(result, ErrorLockResult)
        assert "Failed to retrieve module locks" in result.error_message
        client.views.create.assert_called_once()

    def test_try_lock_create_view_fails_raises(self, client: ToolkitClientMock, git: MagicMock) -> None:
        client.instances.retrieve.side_effect = _view_missing_error()
        client.containers.create.side_effect = ToolkitAPIError("Forbidden", code=403)

        with pytest.raises(ToolkitValueError, match="Failed to create view for module locks"):
            ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])

    def test_try_lock_retrieve_error(self, client: ToolkitClientMock, git: MagicMock) -> None:
        client.instances.retrieve.side_effect = ToolkitAPIError("Internal error", code=500)

        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])

        assert isinstance(result, ErrorLockResult)
        assert "500 - Internal error" in result.error_message
        client.views.create.assert_not_called()

    def test_try_lock_create_error(self, client: ToolkitClientMock, git: MagicMock) -> None:
        client.instances.create.side_effect = ToolkitAPIError("Version conflict", code=409)

        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])

        assert isinstance(result, ErrorLockResult)
        assert "Failed to create module locks" in result.error_message

    def test_try_lock_git_name_fallback(self, client: ToolkitClientMock, git: MagicMock) -> None:
        git.get_me.side_effect = RuntimeError("no user.name")

        result = ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])

        assert isinstance(result, SuccessfulLockResult)
        assert result.git_name.startswith("unknown-")

    @pytest.mark.parametrize("failing_method", ["get_current_branch", "get_default_branch"])
    def test_try_lock_branch_lookup_fails(self, failing_method: str, client: ToolkitClientMock, git: MagicMock) -> None:
        getattr(git, failing_method).side_effect = RuntimeError("git failed")

        with pytest.raises(ToolkitValueError, match="Failed to retrieve"):
            ModuleLockManager(client, git, dry_run=False).try_lock(["m1"])


class TestModuleLockManagerReleaseLock:
    @staticmethod
    def _successful(module_ids: list[str], is_deploy_branch: bool = True) -> SuccessfulLockResult:
        return SuccessfulLockResult(
            module_ids=module_ids,
            is_deploy_branch=is_deploy_branch,
            user_id=USER_ID,
            git_name=GIT_NAME,
            current_branch=DEPLOY_BRANCH if is_deploy_branch else FEATURE_BRANCH,
        )

    def test_release_lock_not_deploy_branch(self, client: ToolkitClientMock, git: MagicMock) -> None:
        released = ModuleLockManager(client, git, dry_run=False).release_lock(self._successful(["m1"], False))

        assert released is False
        client.instances.create.assert_not_called()

    @pytest.mark.parametrize(
        "dry_run, module_ids",
        [
            pytest.param(True, ["m1"], id="Dry run"),
            pytest.param(False, [], id="No modules"),
        ],
    )
    def test_release_lock_noop(
        self, dry_run: bool, module_ids: list[str], client: ToolkitClientMock, git: MagicMock
    ) -> None:
        released = ModuleLockManager(client, git, dry_run=dry_run).release_lock(self._successful(module_ids))

        assert released is True
        client.instances.create.assert_not_called()

    def test_release_lock(self, client: ToolkitClientMock, git: MagicMock) -> None:
        released = ModuleLockManager(client, git, dry_run=False).release_lock(self._successful(["m1", "m2"]))

        assert released is True
        nodes = _created_nodes(client)
        assert [node.external_id for node in nodes] == ["m1", "m2"]
        assert all(node.existing_version is None for node in nodes)
        for node in nodes:
            assert node.sources is not None
            properties = node.sources[0].properties
            assert properties is not None
            assert properties["lock"] is False
            assert properties["branch"] == DEPLOY_BRANCH
        assert client.instances.create.call_args.kwargs == {"replace": True}

    @pytest.mark.parametrize(
        "error, expected",
        [
            pytest.param(_view_missing_error(), True, id="View missing"),
            pytest.param(ToolkitAPIError("Internal error", code=500), False, id="Server error"),
        ],
    )
    def test_release_lock_api_error(
        self, error: ToolkitAPIError, expected: bool, client: ToolkitClientMock, git: MagicMock
    ) -> None:
        client.instances.create.side_effect = error

        released = ModuleLockManager(client, git, dry_run=False).release_lock(self._successful(["m1"]))

        assert released is expected


class TestIsViewMissing:
    @pytest.mark.parametrize(
        "error, attempt, expected",
        [
            pytest.param(ToolkitAPIError("View does not exist", code=400), 0, True, id="Singular"),
            pytest.param(ToolkitAPIError("One or more views do not exist", code=400), 0, True, id="Plural"),
            pytest.param(ToolkitAPIError("Views do not exist", code=404), 0, True, id="404"),
            pytest.param(ToolkitAPIError("Views do not exist", code=400), 1, False, id="Second attempt"),
            pytest.param(ToolkitAPIError("Views do not exist", code=500), 0, False, id="Server error"),
            pytest.param(ToolkitAPIError("Views do not exist", code=None), 0, False, id="No code"),
            pytest.param(ToolkitAPIError("Space does not exist", code=400), 0, False, id="Other message"),
        ],
    )
    def test_is_view_missing(self, error: ToolkitAPIError, attempt: int, expected: bool) -> None:
        assert ModuleLockManager._is_view_missing(attempt, error) is expected
