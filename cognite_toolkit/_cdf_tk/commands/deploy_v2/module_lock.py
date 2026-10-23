from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

from pydantic import BaseModel

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ViewId, NodeId
from cognite_toolkit._cdf_tk.client.resource_classes.data_modeling import InstanceResponse
from cognite_toolkit._cdf_tk.utils.git_helpers import Git
from tests.test_unit.test_cdf_tk.test_rules.test_data_modeling import CONTAINER_ID

SPACE = "cognite_toolkit"
EXTERNAL_ID = "ModuleLock"
VERSION = "v1"

VIEW_ID = ViewId(space=SPACE, external_id=EXTERNAL_ID, version=VERSION)


@dataclass
class FailedLock:
    module_id: str
    owner_id: str
    git_config: str
    user_id: str
    user_email: str | None = None

@dataclass
class ModulesLockResult:
    """Represents the lock state of a set of modules.

    Attributes:
        module_ids: List of module IDs that could not be locked.
    """

    module_ids: list[str]
    current_branch: str
    deploy_branch: str
    failed_locks: list[FailedLock] | None = None

class ModuleLock(BaseModel):
    """Represents a lock on a module."""
    module_id: str
    owner_id: str
    timestamp: datetime
    git_config: str
    user_id: str
    lock: bool
    existing_version: int | None = None

    @classmethod
    def from_nodes(cls, nodes: list[InstanceResponse]) -> list["ModuleLock"]:
        """Parse a list of nodes into a list of ModuleLock objects."""
        raise NotImplementedError("This method should be implemented to parse nodes into ModuleLock objects.")


class ModuleLockManager:
    def __init__(self, client: ToolkitClient, git: Git) -> None:
        self.client = client
        self.git = git

    def try_lock(self, module_ids: list[str],) -> ModulesLockResult:
        """Try to lock the modules in the list. Return a ModulesLockResult object containing the results."""
        return self._try_lock(module_ids, )

    def _try_lock(self, module_ids: list[str], attempt: int = 0) -> ModulesLockResult:
        try:
            module_nodes = self.client.instances.retrieve([NodeId(space=SPACE, external_id=module_id) for module_id in module_ids], source=VIEW_ID)
        except ToolkitAPIError as e:
            if e.status_code == 404 and attempt == 0:
                self._create_lock_view()
                return self._try_lock(module_ids, attempt=1)
            else:
                raise e
        module_lock_by_id ={lock.module_id: lock for lock in  ModuleLock.from_nodes(module_nodes)}
        attempt_lock: list[str] = []
        failed_locks: list[FailedLock] = []
        for module_id in module_ids:
            if module_id not in module_lock_by_id:
                attempt_lock.append(module_id)
                continue
            lock = module_lock_by_id[module_id]
            if self._can_attempt_lock(lock):
                attempt_lock.append(module_id)
            else:
                failed_locks.append(FailedLock(module_id=module_id, owner_id=lock.owner_id, git_config=lock.git_config, user_id=lock.user_id, user_email=self.client.lookup.users.email(lock.user_id)))
        if failed_locks:
            return ModulesLockResult(failed_locks=failed_locks)




        return ModulesLockResult(module_ids=module_ids, current_branch="my_feature", deploy_branch="main")

    def release_lock(self, lock: ModulesLockResult) -> None:
        """Release the locks on the modules in the list."""
        ...

    def _can_attempt_lock(self, lock: ModuleLock) -> bool:
        """Check if the lock can be attempted."""
        ...

    def _create_lock_view(self) -> None:
        """Create the lock view if it does not exist."""
        ...
