from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cached_property

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    InstanceRequest,
    InstanceResponse,
    InstanceSource,
    NodeRequest,
)
from cognite_toolkit._cdf_tk.utils.git_helpers import Git
from cognite_toolkit._cdf_tk.utils.user import UserInfo

SPACE = "cognite_toolkit"
EXTERNAL_ID = "ModuleLock"
VERSION = "v1"

VIEW_ID = ViewId(space=SPACE, external_id=EXTERNAL_ID, version=VERSION)

HEARTBEAT_INTERVAL_SECONDS = 24 * 60 * 60  # 24 hours


@dataclass
class FailedLock:
    module_id: str
    git_name: str
    user_id: str
    user_email: str | None = None


@dataclass
class ModulesLockResult:
    """Represents the lock state of a set of modules."""


@dataclass
class SuccessfulLockResult(ModulesLockResult):
    module_ids: list[str]
    current_branch: str
    deploy_branch: str
    user_id: str
    git_name: str


@dataclass
class FailedLockResult(ModulesLockResult):
    failed_locks: list[FailedLock]


@dataclass
class ErrorLockResult(ModulesLockResult):
    error_message: str


class ModuleLock(BaseModel):
    """Represents a lock on a module."""

    module_id: str
    timestamp: datetime
    git_name: str
    user_id: str
    lock: bool
    existing_version: int | None = None

    @property
    def is_expired(self) -> bool:
        """Check if the lock has expired based on the timestamp and heartbeat interval."""
        return (datetime.now(UTC) - self.timestamp).total_seconds() > HEARTBEAT_INTERVAL_SECONDS

    @classmethod
    def from_nodes(cls, nodes: list[InstanceResponse]) -> list["ModuleLock"]:
        """Parse a list of nodes into a list of ModuleLock objects."""
        raise NotImplementedError("This method should be implemented to parse nodes into ModuleLock objects.")

    @classmethod
    def as_nodes(cls, locks: list["ModuleLock"]) -> list[InstanceRequest]:
        """Convert a list of ModuleLock objects into a list of nodes."""
        raise NotImplementedError("This method should be implemented to convert ModuleLock objects into nodes.")


class ModuleLockManager:
    def __init__(self, client: ToolkitClient, git: Git, deploy_branch: str | None = None) -> None:
        self._client = client
        self._git = git
        self._deploy_branch = deploy_branch

    def try_lock(
        self,
        module_ids: list[str],
    ) -> ModulesLockResult:
        """Try to lock the modules in the list. Return a ModulesLockResult object containing the results."""
        return self._try_lock(module_ids, attempt=0)

    @cached_property
    def _current_branch(self) -> str:
        return self._git.get_current_branch()

    @cached_property
    def deploy_branch(self) -> str:
        if self._deploy_branch is not None:
            return self._deploy_branch
        return self._git.get_default_branch()

    @cached_property
    def _git_name(self) -> str:
        return self._git.get_me()

    def _try_lock(self, module_ids: list[str], attempt: int = 0) -> ModulesLockResult:
        if not self._git.is_git_repo():
            return ErrorLockResult(error_message="Not a git repository. This is required to lock modules.")
        try:
            user_id = UserInfo.load(self._client).id
        except ToolkitAPIError as e:
            return ErrorLockResult(
                error_message=f"Failed to retrieve user info due to API error: {e.code} - {e.message}"
            )
        if user_id is None:
            return ErrorLockResult(error_message="Failed to retrieve user info: user ID is None.")
        if self.deploy_branch == self._current_branch:
            # If the deploy branch is the same as the current branch, we always allow the lock to be acquired.
            return SuccessfulLockResult(
                module_ids=module_ids,
                current_branch=self._current_branch,
                deploy_branch=self.deploy_branch,
                user_id=user_id,
                git_name=self._git_name,
            )
        try:
            module_nodes = self._client.instances.retrieve(
                [NodeId(space=SPACE, external_id=module_id) for module_id in module_ids], source=VIEW_ID
            )
        except ToolkitAPIError as e:
            if e.code == 404 and attempt == 0:
                self._create_lock_view()
                return self._try_lock(module_ids, attempt=1)
            else:
                return ErrorLockResult(
                    error_message=f"Failed to retrieve module locks due to API error: {e.code} - {e.message}"
                )
        module_lock_by_id = {lock.module_id: lock for lock in ModuleLock.from_nodes(module_nodes)}

        attempt_lock: list[ModuleLock] = []
        failed_locks: list[FailedLock] = []
        for module_id in module_ids:
            if module_id not in module_lock_by_id:
                attempt_lock.append(self._create_lock(module_id, user_id))
                continue
            lock = module_lock_by_id[module_id]
            if self._can_attempt_lock(lock, user_id):
                attempt_lock.append(self._create_lock(module_id, user_id, lock))
            else:
                failed_locks.append(
                    FailedLock(
                        module_id=module_id,
                        git_name=lock.git_name,
                        user_id=lock.user_id,
                        user_email=self._client.lookup.users.email(lock.user_id),
                    )
                )

        if failed_locks:
            return FailedLockResult(failed_locks=failed_locks)
        try:
            _ = self._client.instances.create(ModuleLock.as_nodes(attempt_lock), replace=True)
        except ToolkitAPIError as e:
            return ErrorLockResult(
                error_message=f"Failed to create module locks due to API error: {e.code} - {e.message}"
            )

        return SuccessfulLockResult(
            module_ids=module_ids,
            current_branch=self._current_branch,
            deploy_branch=self.deploy_branch,
            user_id=user_id,
            git_name=self._git_name,
        )

    def release_lock(self, lock: SuccessfulLockResult) -> None:
        """Release the locks on the modules in the list."""
        if self.deploy_branch != self._current_branch:
            return
        # We only release locks if we are on the deploy branch, otherwise we assume the locks are still valid
        # as we expect development is still ongoing on the current branch.
        try:
            self._client.instances.create(
                [
                    NodeRequest(
                        space=SPACE,
                        external_id=item,
                        sources=[InstanceSource(source=VIEW_ID, properties={"lock": False})],
                    )
                    for item in lock.module_ids
                ],
                replace=False,
            )
        except ToolkitAPIError as e:
            if e.code == 404:
                # Mock view does not exist, so we don't need to release locks.
                return
            self._client.console.print(f"Failed to release module locks due to API error: {e.code} - {e.message}")

    def _can_attempt_lock(self, lock: ModuleLock, user_id: str) -> bool:
        """Check if the lock can be attempted."""
        return lock.lock is False or lock.is_expired or (lock.git_name == self._git_name and lock.user_id == user_id)

    def _create_lock_view(self) -> None:
        """Create the lock view if it does not exist."""
        raise NotImplementedError(
            "Create the view used to store module locks. This should be implemented to create the view in the CDF."
        )

    def _create_lock(self, module_id: str, user_id: str, existing_lock: ModuleLock | None = None) -> ModuleLock:
        """Create a lock for the module."""
        return ModuleLock(
            module_id=module_id,
            timestamp=datetime.now(UTC),
            git_name=self._git_name,
            user_id=user_id,
            lock=True,
            existing_version=existing_lock.existing_version if existing_lock else None,
        )
