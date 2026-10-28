import re
from dataclasses import dataclass
from datetime import UTC, datetime
from functools import cached_property
from typing import Self

from pydantic import BaseModel, ValidationError
from pydantic.alias_generators import to_camel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client._types import DMSTimestamp
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ContainerId, NodeId, ViewId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import (
    BooleanProperty,
    ContainerPropertyDefinition,
    ContainerRequest,
    InstanceRequest,
    InstanceResponse,
    InstanceSource,
    NodeRequest,
    SpaceRequest,
    TextProperty,
    TimestampProperty,
    ViewCorePropertyRequest,
    ViewRequest,
)
from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError
from cognite_toolkit._cdf_tk.utils import get_cicd_environment
from cognite_toolkit._cdf_tk.utils.git_helpers import Git
from cognite_toolkit._cdf_tk.utils.user import UserInfo
from cognite_toolkit._cdf_tk.validation import humanize_validation_error

SPACE = "cognite_toolkit"
EXTERNAL_ID = "ModuleLock"
VERSION = "v1"

VIEW_ID = ViewId(space=SPACE, external_id=EXTERNAL_ID, version=VERSION)

HEARTBEAT_INTERVAL_SECONDS = 24 * 60 * 60  # 24 hours
# Limit for the number of modules to lock at once to avoid hitting API limits or performance issues.
MAX_MODULE_COUNT = 1000  # Maximum number of modules to lock at once


@dataclass
class FailedLock:
    module_id: str
    git_name: str
    user_id: str
    branch: str
    user_email: str | None = None

    @property
    def display_email(self) -> str:
        """Return the user email in angle brackets if it exists, otherwise return an empty string."""
        return f" <{self.user_email}>" if self.user_email else ""


@dataclass
class ModulesLockResult:
    """Represents the lock state of a set of modules."""


@dataclass
class SuccessfulLockResult(ModulesLockResult):
    module_ids: list[str]
    is_deploy_branch: bool
    user_id: str
    git_name: str
    current_branch: str
    repo: str | None = None


@dataclass
class FailedLockResult(ModulesLockResult):
    failed_locks: list[FailedLock]


@dataclass
class ErrorLockResult(ModulesLockResult):
    error_message: str


class ModuleLock(BaseModel, alias_generator=to_camel, populate_by_name=True):
    """Represents a lock on a module."""

    module_id: str
    timestamp: DMSTimestamp
    git_name: str
    user_id: str
    branch: str
    lock: bool
    existing_version: int | None
    repo: str | None = None

    @property
    def is_expired(self) -> bool:
        """Check if the lock has expired based on the timestamp and heartbeat interval."""
        return (datetime.now(UTC) - self.timestamp).total_seconds() > HEARTBEAT_INTERVAL_SECONDS

    @classmethod
    def from_nodes(cls, nodes: list[InstanceResponse]) -> list[Self]:
        """Parse a list of nodes into a list of ModuleLock objects."""
        locks: list[Self] = []
        for node in nodes:
            if node.properties is None:
                continue
            properties = node.properties.get(VIEW_ID)
            if properties is None:
                continue
            try:
                lock = cls.model_validate({**properties, "existingVersion": node.version})
            except ValidationError as e:
                errors = humanize_validation_error(e, ModuleLock)
                error_str = "\n - ".join(errors)
                raise ToolkitValueError(f"Failed to parse module lock from node {node.external_id}: {error_str}") from e
            locks.append(lock)
        return locks

    @classmethod
    def as_nodes(cls, locks: list["ModuleLock"]) -> list[InstanceRequest]:
        """Convert a list of ModuleLock objects into a list of nodes."""
        return [
            NodeRequest(
                space=SPACE,
                external_id=lock.module_id,
                existing_version=lock.existing_version,
                sources=[
                    InstanceSource(
                        source=VIEW_ID,
                        properties=lock.model_dump(mode="json", by_alias=True, exclude={"existing_version"}),
                    )
                ],
            )
            for lock in locks
        ]


class ModuleLockManager:
    def __init__(self, client: ToolkitClient, git: Git, dry_run: bool, deploy_branch: str | None = None) -> None:
        self._client = client
        self._git = git
        self._dry_run = dry_run
        self._deploy_branch = deploy_branch

    def try_lock(
        self,
        module_ids: list[str],
    ) -> ModulesLockResult:
        """Try to lock the modules in the list. Return a ModulesLockResult object containing the results."""
        if len(module_ids) > MAX_MODULE_COUNT:
            return ErrorLockResult(error_message=f"Cannot lock more than {MAX_MODULE_COUNT} modules at once.")
        if not self._git.is_git_repo():
            return ErrorLockResult(error_message="Not a git repository. This is required to lock modules.")
        user_id = UserInfo.load(self._client).id
        if user_id is None:
            return ErrorLockResult(error_message="Failed to retrieve user info: user ID is None.")

        module_ids = list(dict.fromkeys(module_ids))  # Remove duplicates while preserving order
        if len(module_ids) == 0 or self.deploy_branch == self._current_branch:
            # If the deploy branch is the same as the current branch, we always allow the lock to be acquired.
            # Or, if there are no modules to lock, we return a successful lock result with an empty list.
            return SuccessfulLockResult(
                module_ids=module_ids,
                is_deploy_branch=self.deploy_branch == self._current_branch,
                user_id=user_id,
                git_name=self._git_name,
                current_branch=self._current_branch,
                repo=self._repo,
            )

        return self._try_lock(module_ids, user_id, attempt=0)

    @cached_property
    def _current_branch(self) -> str:
        try:
            return self._git.get_current_branch()
        except RuntimeError:
            raise ToolkitValueError(
                "Failed to retrieve current git branch. You cannot use the lock feature without a valid git repository."
            )

    @cached_property
    def deploy_branch(self) -> str:
        if self._deploy_branch is not None:
            return self._deploy_branch
        try:
            return self._git.get_default_branch()
        except RuntimeError:
            raise ToolkitValueError(
                "Failed to retrieve default git branch. You cannot use the lock feature without a valid git repository."
            )

    @cached_property
    def _repo(self) -> str | None:
        try:
            return self._git.get_remote_repo()
        except RuntimeError:
            return None

    @cached_property
    def _git_name(self) -> str:
        try:
            return self._git.get_me()
        except RuntimeError:
            return f"unknown-{get_cicd_environment()}"

    def _try_lock(self, module_ids: list[str], user_id: str, attempt: int = 0) -> ModulesLockResult:
        try:
            module_nodes = self._client.instances.retrieve(
                [NodeId(space=SPACE, external_id=module_id) for module_id in module_ids], source=VIEW_ID
            )
        except ToolkitAPIError as e:
            if self._is_view_missing(attempt, e):
                self._create_lock_view()
                return self._try_lock(module_ids, user_id, attempt=1)
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
            if self._can_attempt_lock(lock):
                attempt_lock.append(self._create_lock(module_id, user_id, lock))
            else:
                failed_locks.append(
                    FailedLock(
                        module_id=module_id,
                        git_name=lock.git_name,
                        user_id=lock.user_id,
                        # The user.email method already caches the result, so we can call it multiple times without performance issues.
                        user_email=self._client.lookup.users.email(lock.user_id, warn=False),
                        branch=lock.branch,
                    )
                )

        if failed_locks:
            return FailedLockResult(failed_locks=failed_locks)

        if self._dry_run:
            return SuccessfulLockResult(
                module_ids=module_ids,
                is_deploy_branch=False,
                user_id=user_id,
                current_branch=self._current_branch,
                repo=self._repo,
                git_name=self._git_name,
            )

        try:
            _ = self._client.instances.create(ModuleLock.as_nodes(attempt_lock), replace=True)
        except ToolkitAPIError as e:
            return ErrorLockResult(
                error_message=f"Failed to create module locks due to API error: {e.code} - {e.message}"
            )

        return SuccessfulLockResult(
            module_ids=module_ids,
            is_deploy_branch=False,
            user_id=user_id,
            current_branch=self._current_branch,
            repo=self._repo,
            git_name=self._git_name,
        )

    @staticmethod
    def _is_view_missing(attempt: int, e: ToolkitAPIError) -> bool:
        return (
            e.code is not None
            and 400 <= e.code < 500
            and attempt == 0
            and bool(re.search(r"views? do(es)? not exist", e.message.lower()))
        )

    def release_lock(self, lock: SuccessfulLockResult) -> bool:
        """Release the locks on the modules in the list."""
        if not lock.is_deploy_branch:
            return False
        if self._dry_run or len(lock.module_ids) == 0:
            return True
        # We only release locks if we are on the deploy branch, otherwise we assume the locks are still valid
        # as we expect development is still ongoing on the current branch.
        try:
            self._client.instances.create(
                ModuleLock.as_nodes([self._create_lock_release(item, lock) for item in lock.module_ids]),
                replace=True,
            )
        except ToolkitAPIError as e:
            if self._is_view_missing(0, e):
                # Mock view does not exist, so we don't need to release locks.
                return True
            self._client.console.print(f"Failed to release module locks due to API error: {e.code} - {e.message}")
            return False
        return True

    @classmethod
    def _create_lock_release(cls, module_id: str, lock: SuccessfulLockResult) -> ModuleLock:
        return ModuleLock(
            module_id=module_id,
            timestamp=datetime.now(UTC),
            git_name=lock.git_name,
            user_id=lock.user_id,
            branch=lock.current_branch,
            repo=lock.repo,
            lock=False,
            existing_version=None,
        )

    def _can_attempt_lock(self, lock: ModuleLock) -> bool:
        """Check if the lock can be attempted."""
        return lock.lock is False or lock.is_expired or self._is_same_branch(lock)

    def _is_same_branch(self, lock: ModuleLock) -> bool:
        """The lock is held by the current branch. If both the lock and the current repository are known,
        the repository must match as well, as two repositories may have branches with the same name."""
        if lock.branch != self._current_branch:
            return False
        return lock.repo is None or self._repo is None or lock.repo == self._repo

    def _create_lock_view(self) -> None:
        """Create the lock view if it does not exist."""
        container_properties: dict[str, ContainerPropertyDefinition] = {
            "moduleId": ContainerPropertyDefinition(nullable=False, type=TextProperty()),
            "timestamp": ContainerPropertyDefinition(nullable=False, type=TimestampProperty()),
            "gitName": ContainerPropertyDefinition(nullable=False, type=TextProperty()),
            "userId": ContainerPropertyDefinition(nullable=False, type=TextProperty()),
            "branch": ContainerPropertyDefinition(nullable=False, type=TextProperty()),
            "lock": ContainerPropertyDefinition(nullable=False, type=BooleanProperty()),
            "repo": ContainerPropertyDefinition(nullable=True, type=TextProperty()),
        }
        try:
            self._client.spaces.create(
                [
                    SpaceRequest(
                        space=SPACE,
                        name="Cognite Toolkit",
                        description="Cognite Toolkit internal space for module locks",
                    )
                ]
            )
            self._client.containers.create(
                [
                    ContainerRequest(
                        space=SPACE,
                        external_id=EXTERNAL_ID,
                        name="Module Lock",
                        description="Cognite Toolkit internal container for module locks",
                        used_for="node",
                        properties=container_properties,
                    )
                ]
            )
            self._client.views.create(
                [
                    ViewRequest(
                        space=SPACE,
                        external_id=EXTERNAL_ID,
                        version=VERSION,
                        name="Module Lock View",
                        description="Cognite Toolkit internal view for module locks",
                        properties={
                            prop_id: ViewCorePropertyRequest(
                                container=ContainerId(space=SPACE, external_id=EXTERNAL_ID),
                                container_property_identifier=prop_id,
                            )
                            for prop_id in container_properties.keys()
                        },
                    )
                ]
            )
        except ToolkitAPIError as e:
            raise ToolkitValueError(
                f"Failed to create view for module locks due to API error: {e.code} - {e.message}"
            ) from e

    def _create_lock(self, module_id: str, user_id: str, existing_lock: ModuleLock | None = None) -> ModuleLock:
        """Create a lock for the module."""
        return ModuleLock(
            module_id=module_id,
            timestamp=datetime.now(UTC),
            git_name=self._git_name,
            user_id=user_id,
            lock=True,
            branch=self._current_branch,
            repo=self._repo,
            existing_version=existing_lock.existing_version if existing_lock else 0,
        )
