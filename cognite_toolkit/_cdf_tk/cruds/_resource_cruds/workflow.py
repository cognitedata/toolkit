# Copyright 2023 Cognite AS
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import json
from collections.abc import Hashable, Iterable, Sequence
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes import ClientCredentials
from cognite.client.data_classes.capabilities import (
    Capability,
    WorkflowOrchestrationAcl,
)
from rich.console import Console

from cognite_toolkit._cdf_tk.client import ToolkitClient
from cognite_toolkit._cdf_tk.client.http_client import ToolkitAPIError
from cognite_toolkit._cdf_tk.client.identifiers import ExternalId, WorkflowVersionId
from cognite_toolkit._cdf_tk.client.resource_classes.workflow import WorkflowRequest, WorkflowResponse
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_trigger import (
    NonceCredentials,
    WorkflowTriggerRequest,
    WorkflowTriggerResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.workflow_version import (
    SubworkflowTaskParameters,
    WorkflowVersionRequest,
    WorkflowVersionResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.exceptions import (
    ResourceCreationError,
    ToolkitCycleError,
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.resource_classes import WorkflowTriggerYAML, WorkflowVersionYAML, WorkflowYAML
from cognite_toolkit._cdf_tk.tk_warnings import (
    MissingReferencedWarning,
    ToolkitWarning,
)
from cognite_toolkit._cdf_tk.utils import (
    calculate_secure_hash,
    humanize_collection,
    load_yaml_inject_variables,
    sanitize_filename,
)
from cognite_toolkit._cdf_tk.utils.cdf import read_auth, try_find_error
from cognite_toolkit._cdf_tk.utils.diff_list import diff_list_hashable, diff_list_identifiable

from .auth import GroupAllScopedCRUD
from .data_organization import DataSetsCRUD
from .function import FunctionCRUD
from .group_scoped import GroupResourceScopedCRUD
from .transformation import TransformationCRUD


@final
class WorkflowCRUD(ResourceCRUD[ExternalId, WorkflowRequest, WorkflowResponse]):
    folder_name = "workflows"
    resource_cls = WorkflowResponse
    resource_write_cls = WorkflowRequest
    kind = "Workflow"
    dependencies = frozenset(
        {
            GroupAllScopedCRUD,
            TransformationCRUD,
            FunctionCRUD,
            DataSetsCRUD,
        }
    )
    yaml_cls = WorkflowYAML
    _doc_base_url = "https://api-docs.cognite.com/20230101-beta/tag/"
    _doc_url = "Workflows/operation/CreateOrUpdateWorkflow"

    @property
    def display_name(self) -> str:
        return "workflows"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[WorkflowRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [WorkflowOrchestrationAcl.Action.Read]
            if read_only
            else [WorkflowOrchestrationAcl.Action.Read, WorkflowOrchestrationAcl.Action.Write]
        )

        return WorkflowOrchestrationAcl(
            actions,
            WorkflowOrchestrationAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: WorkflowRequest | WorkflowResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if item.external_id is None:
            raise ToolkitRequiredValueError("Workflow must have external_id set.")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> WorkflowRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return WorkflowRequest.model_validate(resource)

    def dump_resource(self, resource: WorkflowResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        return dumped

    def retrieve(self, ids: Sequence[ExternalId]) -> list[WorkflowResponse]:
        return self.client.tool.workflows.retrieve(list(ids), ignore_unknown_ids=True)

    def create(self, items: Sequence[WorkflowRequest]) -> list[WorkflowResponse]:
        return self.client.tool.workflows.create(items)

    def update(self, items: Sequence[WorkflowRequest]) -> list[WorkflowResponse]:
        return self.client.tool.workflows.update(items)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.workflows.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[WorkflowResponse]:
        if data_set_external_id is None:
            for workflows in self.client.tool.workflows.iterate(limit=100):
                yield from workflows
            return
        data_sets = self.client.tool.datasets.retrieve(
            [ExternalId(external_id=data_set_external_id)], ignore_unknown_ids=True
        )
        if not data_sets:
            raise ToolkitRequiredValueError(f"DataSet {data_set_external_id!r} does not exist")
        data_set = data_sets[0]
        for workflows in self.client.tool.workflows.iterate(limit=100):
            for workflow in workflows:
                if workflow.data_set_id == data_set.id:
                    yield workflow

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, ExternalId(external_id=item["dataSetExternalId"])


@final
class WorkflowVersionCRUD(ResourceCRUD[WorkflowVersionId, WorkflowVersionRequest, WorkflowVersionResponse]):
    folder_name = "workflows"
    resource_cls = WorkflowVersionResponse
    resource_write_cls = WorkflowVersionRequest
    kind = "WorkflowVersion"
    dependencies = frozenset({WorkflowCRUD})
    parent_resource = frozenset({WorkflowCRUD})
    yaml_cls = WorkflowVersionYAML

    _doc_base_url = "https://api-docs.cognite.com/20230101-beta/tag/"
    _doc_url = "Workflow-versions/operation/CreateOrUpdateWorkflowVersion"
    # The /list endpoint has a limit of 100 workflow ids per request.
    _list_workflow_filter_limit = 100

    @property
    def display_name(self) -> str:
        return "workflow versions"

    @classmethod
    def get_required_capability(
        cls, items: Sequence[WorkflowVersionRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = (
            [WorkflowOrchestrationAcl.Action.Read]
            if read_only
            else [WorkflowOrchestrationAcl.Action.Read, WorkflowOrchestrationAcl.Action.Write]
        )

        return WorkflowOrchestrationAcl(
            actions,
            WorkflowOrchestrationAcl.Scope.All(),
        )

    @classmethod
    def get_id(cls, item: WorkflowVersionRequest | WorkflowVersionResponse | dict) -> WorkflowVersionId:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"workflowExternalId", "version"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return WorkflowVersionId(workflow_external_id=item["workflowExternalId"], version=item["version"])
        return item.as_id()

    @classmethod
    def dump_id(cls, id: WorkflowVersionId) -> dict[str, Any]:
        return id.dump()

    def load_resource_file(
        self,
        filepath: Path,
        environment_variables: dict[str, str | None] | None = None,
    ) -> list[dict[str, Any]]:
        # Special case where the environment variable keys in the 'workflowDefinition.tasks.parameters' are
        # JSONPaths that are part of the API and thus should not be replaced.
        raw_str = self.safe_read(filepath)

        original = load_yaml_inject_variables(raw_str, {}, validate=False, original_filepath=filepath)
        replaced = load_yaml_inject_variables(
            raw_str, environment_variables or {}, validate=False, original_filepath=filepath
        )

        if isinstance(original, dict) and isinstance(replaced, dict):
            self._update_task_parameters(replaced, original)
            return [replaced]
        elif isinstance(original, list) and isinstance(replaced, list):
            for original_item, replaced_item in zip(original, replaced):
                self._update_task_parameters(replaced_item, original_item)
            return replaced
        else:
            # Should be unreachable
            raise ValueError(
                f"Unexpected state. Loaded {filepath.as_posix()!r} twice and got different types: {type(original)} and {type(replaced)}"
            )

    @staticmethod
    def _update_task_parameters(replaced: dict, original: dict) -> None:
        replaced_def = replaced.get("workflowDefinition")
        original_def = original.get("workflowDefinition")
        if not (isinstance(replaced_def, dict) and isinstance(original_def, dict)):
            return
        replaced_tasks = replaced_def.get("tasks")
        original_tasks = original_def.get("tasks")
        if not (isinstance(replaced_tasks, list) and isinstance(original_tasks, list)):
            return
        for replaced_task, original_task in zip(replaced_def["tasks"], original_def["tasks"]):
            if not (isinstance(replaced_task, dict) and isinstance(original_task, dict)):
                continue
            if "parameters" in replaced_task and "parameters" in original_task:
                replaced_task["parameters"] = original_task["parameters"]

    def dump_resource(self, resource: WorkflowVersionResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        if not local:
            return dumped
        # Sort to match the order of the local tasks
        if "workflowDefinition" in local and "tasks" in local["workflowDefinition"]:
            local_task_order_by_id = {
                task["externalId"]: no for no, task in enumerate(local["workflowDefinition"]["tasks"])
            }
            local_task_by_id = {task["externalId"]: task for task in local["workflowDefinition"]["tasks"]}
        else:
            local_task_order_by_id = {}
            local_task_by_id = {}
        end_of_list = len(local_task_order_by_id)
        dumped["workflowDefinition"]["tasks"] = sorted(
            dumped["workflowDefinition"]["tasks"],
            key=lambda t: local_task_order_by_id.get(t["externalId"], end_of_list),
        )

        # Function tasks with empty data can be an empty dict or missing data field
        # This ensures that these two are treated as the same
        for cdf_task in dumped["workflowDefinition"]["tasks"]:
            task_id = cdf_task["externalId"]
            if task_id not in local_task_by_id:
                continue
            local_task = local_task_by_id[task_id]
            for key, default_value in [
                ("retries", 3),
                ("timeout", 3600),
                ("onFailure", "abortWorkflow"),
                ("dependsOn", []),
            ]:
                if key not in local_task and cdf_task.get(key) == default_value:
                    del cdf_task[key]
                elif (
                    key in local_task
                    and local_task[key] is None
                    and (cdf_task.get(key) == default_value or key not in cdf_task)
                ):
                    cdf_task[key] = None

            if local_task["type"] == "function" and cdf_task["type"] == "function":
                cdf_parameters = cdf_task["parameters"]
                local_parameters = local_task["parameters"]
                if "function" in cdf_parameters and "function" in local_parameters:
                    cdf_function = cdf_parameters["function"]
                    local_function = local_parameters["function"]
                    if local_function.get("data") == {} and "data" not in cdf_function:
                        cdf_parameters["function"] = local_function
                    elif cdf_function.get("data") == {} and "data" not in local_function:
                        del cdf_function["data"]
            elif local_task["type"] == "transformation" and cdf_task["type"] == "transformation":
                cdf_parameters = cdf_task["parameters"]
                local_parameters = local_task["parameters"]
                if "transformation" in cdf_parameters and "transformation" in local_parameters:
                    for key, default_transformation_value in [
                        ("concurrencyPolicy", "fail"),
                        ("useTransformationCredentials", False),
                    ]:
                        if (
                            key not in local_parameters["transformation"]
                            and cdf_parameters["transformation"].get(key) == default_transformation_value
                        ):
                            del cdf_parameters["transformation"][key]
        return dumped

    def diff_list(
        self, local: list[Any], cdf: list[Any], json_path: tuple[str | int, ...]
    ) -> tuple[dict[int, int], list[int]]:
        if json_path == ("workflowDefinition", "tasks"):
            return diff_list_identifiable(local, cdf, get_identifier=lambda t: t["externalId"])
        elif len(json_path) == 4 and json_path[:2] == ("workflowDefinition", "tasks") and json_path[3] == "dependsOn":
            return diff_list_identifiable(local, cdf, get_identifier=lambda t: t["externalId"])
        elif len(json_path) >= 2 and json_path[:2] == ("workflowDefinition", "tasks"):
            # Assume all other arrays in the tasks are hashable
            return diff_list_hashable(local, cdf)
        return super().diff_list(local, cdf, json_path)

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "workflowExternalId" in item:
            yield WorkflowCRUD, ExternalId(external_id=item["workflowExternalId"])

    @classmethod
    def check_item(cls, item: dict, filepath: Path, element_no: int | None) -> list[ToolkitWarning]:
        warnings: list[ToolkitWarning] = []
        tasks = item.get("workflowDefinition", {}).get("tasks", [])
        if not tasks:
            # We do not check for tasks here, that is done by comparing the spec.
            return warnings
        # Checking for invalid dependsOn
        tasks_ids = {task["externalId"] for task in tasks if "externalId" in task}
        for task in tasks:
            if not isinstance(depends_on := task.get("dependsOn"), list):
                continue
            invalid_tasks = [
                dep["externalId"] for dep in depends_on if "externalId" in dep and dep["externalId"] not in tasks_ids
            ]
            if invalid_tasks:
                warnings.append(
                    MissingReferencedWarning(
                        filepath=filepath,
                        element_no=element_no,
                        path=tuple(),
                        message=f"Task {task['externalId']!r} depends on non-existing task(s): {humanize_collection(invalid_tasks)!r}",
                    )
                )
        return warnings

    def retrieve(self, ids: Sequence[WorkflowVersionId]) -> list[WorkflowVersionResponse]:
        if not ids:
            return []
        return self.client.tool.workflows.versions.retrieve(list(ids), ignore_unknown_ids=True)

    def _upsert(self, items: Sequence[WorkflowVersionRequest]) -> list[WorkflowVersionResponse]:
        return self.client.tool.workflows.versions.create(items)

    def create(self, items: Sequence[WorkflowVersionRequest]) -> list[WorkflowVersionResponse]:
        upserted: list[WorkflowVersionResponse] = []
        for item in self.topological_sort(items):
            upserted.extend(self.client.tool.workflows.versions.create([item]))
        return upserted

    def update(self, items: Sequence[WorkflowVersionRequest]) -> list[WorkflowVersionResponse]:
        return self._upsert(items)

    def delete(self, ids: Sequence[WorkflowVersionId]) -> int:
        if not ids:
            return 0
        self.client.tool.workflows.versions.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[WorkflowVersionResponse]:
        # Note: The new API doesn't support filtering by workflow_ids in list, so we iterate over all
        for versions in self.client.tool.workflows.versions.iterate(limit=100):
            if parent_ids is not None:
                workflow_ids = {
                    parent_id.external_id if isinstance(parent_id, ExternalId) else parent_id
                    for parent_id in parent_ids
                    if isinstance(parent_id, (str, ExternalId))
                }
                for version in versions:
                    if version.workflow_external_id in workflow_ids:
                        yield version
            else:
                yield from versions

    @classmethod
    def as_str(cls, id: WorkflowVersionId) -> str:
        if id.version is None:
            version = ""
        elif id.version.startswith("v"):
            version = f"_{id.version}"
        else:
            version = f"_v{id.version}"

        return sanitize_filename(f"{id.workflow_external_id}{version}")

    @classmethod
    def topological_sort(cls, items: Sequence[WorkflowVersionRequest]) -> list[WorkflowVersionRequest]:
        workflow_by_id: dict[WorkflowVersionId, WorkflowVersionRequest] = {item.as_id(): item for item in items}
        dependencies: dict[WorkflowVersionId, set[WorkflowVersionId]] = {}
        for item_id, item in workflow_by_id.items():
            dependencies[item_id] = set()
            for task in item.workflow_definition.tasks:
                if isinstance(task.parameters, SubworkflowTaskParameters):
                    subworkflow = task.parameters.subworkflow
                    if isinstance(subworkflow, WorkflowVersionId):
                        dependencies[item_id].add(subworkflow)

        try:
            return [
                workflow_by_id[item_id]
                for item_id in TopologicalSorter(dependencies).static_order()
                if item_id in workflow_by_id
            ]
        except CycleError as e:
            raise ToolkitCycleError(
                f"Cannot deploy workflows. Cycle detected {e.args} in the 'subworkflow' dependencies of the workflows.",
                *e.args[1:],
            ) from None


@final
class WorkflowTriggerCRUD(ResourceCRUD[ExternalId, WorkflowTriggerRequest, WorkflowTriggerResponse]):
    folder_name = "workflows"
    resource_cls = WorkflowTriggerResponse
    resource_write_cls = WorkflowTriggerRequest
    kind = "WorkflowTrigger"
    dependencies = frozenset({WorkflowCRUD, WorkflowVersionCRUD, GroupResourceScopedCRUD, GroupAllScopedCRUD})
    parent_resource = frozenset({WorkflowCRUD})
    yaml_cls = WorkflowTriggerYAML

    _doc_url = "Workflow-triggers/operation/CreateOrUpdateTriggers"

    class _MetadataKey:
        secret_hash = "cognite-toolkit-auth-hash"

    def __init__(self, client: ToolkitClient, build_dir: Path | None, console: Console | None = None):
        super().__init__(client, build_dir, console)
        self._authentication_by_id: dict[str, ClientCredentials] = {}

    @property
    def display_name(self) -> str:
        return "workflow triggers"

    @classmethod
    def get_id(cls, item: WorkflowTriggerRequest | WorkflowTriggerResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        return ExternalId(external_id=item.external_id)

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def as_str(cls, id: ExternalId) -> str:
        return sanitize_filename(id.external_id)

    @classmethod
    def get_required_capability(
        cls, items: Sequence[WorkflowTriggerRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        capability = (
            [WorkflowOrchestrationAcl.Action.Read]
            if read_only
            else [WorkflowOrchestrationAcl.Action.Read, WorkflowOrchestrationAcl.Action.Write]
        )

        return WorkflowOrchestrationAcl(
            capability,
            WorkflowOrchestrationAcl.Scope.All(),
        )

    def create(self, items: Sequence[WorkflowTriggerRequest]) -> list[WorkflowTriggerResponse]:
        return self._upsert(items)

    def _upsert(self, items: Sequence[WorkflowTriggerRequest]) -> list[WorkflowTriggerResponse]:
        created: list[WorkflowTriggerResponse] = []
        for item in items:
            created_item = self._upsert_item(item)
            if created_item is not None:
                created.append(created_item)
        return created

    def _upsert_item(self, item: WorkflowTriggerRequest) -> WorkflowTriggerResponse | None:
        credentials = self._authentication_by_id.get(item.external_id)
        item.authentication = NonceCredentials(nonce=self.client.iam.sessions.create(credentials).nonce)
        try:
            result = self.client.tool.workflows.triggers.create([item])
            if not result:
                return None
            return result[0]
        except ToolkitAPIError as e:
            if hint := try_find_error(credentials):
                raise ResourceCreationError(f"Failed to create WorkflowTrigger {item.external_id}: {hint}") from e
            raise e

    def retrieve(self, ids: Sequence[ExternalId]) -> list[WorkflowTriggerResponse]:
        all_triggers = self.client.tool.workflows.triggers.list(limit=None)
        lookup = {id_.external_id for id_ in ids}
        return [trigger for trigger in all_triggers if trigger.external_id in lookup]

    def update(self, items: Sequence[WorkflowTriggerRequest]) -> list[WorkflowTriggerResponse]:
        return self._upsert(items)

    def delete(self, ids: Sequence[ExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.workflows.triggers.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: Sequence[Hashable] | None = None,
    ) -> Iterable[WorkflowTriggerResponse]:
        triggers = self.client.tool.workflows.triggers.list(limit=None)
        if parent_ids is not None:
            # Parent = Workflow
            workflow_ids = {
                parent_id.external_id if isinstance(parent_id, ExternalId) else parent_id
                for parent_id in parent_ids
                if isinstance(parent_id, (str, ExternalId))
            }
            return (trigger for trigger in triggers if trigger.workflow_external_id in workflow_ids)
        return triggers

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires."""
        if "workflowExternalId" in item:
            yield WorkflowCRUD, ExternalId(external_id=item["workflowExternalId"])

            if "workflowVersion" in item:
                yield (
                    WorkflowVersionCRUD,
                    WorkflowVersionId(workflow_external_id=item["workflowExternalId"], version=item["workflowVersion"]),
                )

    def load_resource_file(
        self, filepath: Path, environment_variables: dict[str, str | None] | None = None
    ) -> list[dict[str, Any]]:
        resources = super().load_resource_file(filepath, environment_variables)

        # We need to the auth hash calculation here, as the output of the load_resource_file
        # is used to compare with the CDF resource.
        for resource in resources:
            identifier = self.get_id(resource)
            credentials = read_auth(
                resource.get("authentication"),
                self.client.config,
                identifier.external_id,
                "workflow trigger",
                console=self.console,
            )
            self._authentication_by_id[identifier.external_id] = credentials
            if "metadata" not in resource:
                resource["metadata"] = {}
            resource["metadata"][self._MetadataKey.secret_hash] = calculate_secure_hash(
                credentials.dump(camel_case=True), shorten=True
            )
        return resources

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> WorkflowTriggerRequest:
        if isinstance(resource.get("data"), dict):
            resource["data"] = json.dumps(resource["data"])
        # Remove authentication from resource dict as it contains clientId/clientSecret
        # which are stored separately in _authentication_by_id and converted to nonce at creation time
        resource.pop("authentication", None)
        return WorkflowTriggerRequest.model_validate(resource)

    def dump_resource(self, resource: WorkflowTriggerResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        # Dump directly from response, excluding response-only fields
        dumped = resource.dump()
        # Remove response-only fields
        dumped.pop("createdTime", None)
        dumped.pop("lastUpdatedTime", None)
        dumped.pop("isPaused", None)
        # Remove input if None to match local format
        if dumped.get("input") is None:
            dumped.pop("input", None)
        # Remove authentication if None
        if dumped.get("authentication") is None:
            dumped.pop("authentication", None)

        # Ensure triggerType is included in triggerRule (might be excluded due to exclude_unset=True)
        if "triggerRule" in dumped and "triggerType" not in dumped["triggerRule"]:
            dumped["triggerRule"]["triggerType"] = resource.trigger_rule.trigger_type

        local = local or {}
        if isinstance(dumped.get("data"), str) and isinstance(local.get("data"), dict):
            dumped["data"] = json.loads(dumped["data"])

        cdf_rule = dumped.get("triggerRule", {})
        local_rule = local.get("triggerRule", {})
        if cdf_rule.get("triggerType") == "schedule" and local_rule.get("triggerType") == "schedule":
            if cdf_rule.get("timezone") == "UTC" and "timezone" not in local_rule:
                # The server defaults to UTC if not set, so we remove it if the local does not have it set.
                del cdf_rule["timezone"]

        if "authentication" in local:
            # Changes in auth will be detected by the hash. We need to do this to ensure
            # that the pull command works.
            dumped["authentication"] = local["authentication"]
        return dumped

    def sensitive_strings(self, item: WorkflowTriggerRequest) -> Iterable[str]:
        id_ = self.get_id(item)
        if id_.external_id in self._authentication_by_id:
            yield self._authentication_by_id[id_.external_id].client_secret
