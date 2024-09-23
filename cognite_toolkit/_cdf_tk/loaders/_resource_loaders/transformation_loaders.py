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
from __future__ import annotations

from collections.abc import Hashable, Iterable, Sequence
from functools import lru_cache
from pathlib import Path
from typing import Any, cast, final

from cognite.client.data_classes import (
    OidcCredentials,
    Transformation,
    TransformationList,
    TransformationNotification,
    TransformationNotificationList,
    TransformationSchedule,
    TransformationScheduleList,
    TransformationScheduleWrite,
    TransformationScheduleWriteList,
    TransformationWrite,
    TransformationWriteList,
)
from cognite.client.data_classes.capabilities import (
    Capability,
    TransformationsAcl,
)
from cognite.client.data_classes.data_modeling.ids import (
    DataModelId,
    ViewId,
)
from cognite.client.data_classes.transformations.notifications import (
    TransformationNotificationWrite,
    TransformationNotificationWriteList,
)
from cognite.client.exceptions import CogniteAPIError, CogniteDuplicatedError, CogniteNotFoundError
from cognite.client.utils.useful_types import SequenceNotStr
from rich import print

from cognite_toolkit._cdf_tk._parameters import ANY_INT, ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitInvalidParameterNameError,
    ToolkitRequiredValueError,
    ToolkitYAMLFormatError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.loaders.data_classes import RawDatabaseTable
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    in_dict,
    load_yaml_inject_variables,
    safe_read,
)

from .auth_loaders import GroupAllScopedLoader
from .data_organization_loaders import DataSetsLoader
from .datamodel_loaders import DataModelLoader, SpaceLoader, ViewLoader
from .raw_loaders import RawDatabaseLoader, RawTableLoader


@final
class TransformationLoader(
    ResourceLoader[str, TransformationWrite, Transformation, TransformationWriteList, TransformationList]
):
    folder_name = "transformations"
    filename_pattern = (
        # Matches all yaml files except file names whose stem contain *.schedule. or .Notification
        r"^(?!.*schedule.*|.*\.notification$).*$"
    )
    resource_cls = Transformation
    resource_write_cls = TransformationWrite
    list_cls = TransformationList
    list_write_cls = TransformationWriteList
    kind = "Transformation"
    dependencies = frozenset({DataSetsLoader, RawDatabaseLoader, GroupAllScopedLoader})
    _doc_url = "Transformations/operation/createTransformations"

    @classmethod
    def get_required_capability(cls, items: TransformationWriteList | None) -> Capability | list[Capability]:
        if not items and items is not None:
            return []
        scope: TransformationsAcl.Scope.All | TransformationsAcl.Scope.DataSet = TransformationsAcl.Scope.All()  # type: ignore[valid-type]
        if items is not None:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = TransformationsAcl.Scope.DataSet(list(data_set_ids))

        return TransformationsAcl(
            [TransformationsAcl.Action.Read, TransformationsAcl.Action.Write],
            scope,  # type: ignore[arg-type]
        )

    @classmethod
    def get_id(cls, item: Transformation | TransformationWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("Transformation must have external_id set.")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]
        if destination := item.get("destination", {}):
            if not isinstance(destination, dict):
                return
            if destination.get("type") == "raw" and in_dict(("database", "table"), destination):
                yield RawDatabaseLoader, RawDatabaseTable(destination["database"])
                yield RawTableLoader, RawDatabaseTable(destination["database"], destination["table"])
            elif destination.get("type") in ("nodes", "edges") and (view := destination.get("view", {})):
                if space := destination.get("instanceSpace"):
                    yield SpaceLoader, space
                if in_dict(("space", "externalId", "version"), view):
                    yield ViewLoader, ViewId.load(view)
            elif destination.get("type") == "instances":
                if space := destination.get("instanceSpace"):
                    yield SpaceLoader, space
                if data_model := destination.get("dataModel"):
                    if in_dict(("space", "externalId", "version"), data_model):
                        yield DataModelLoader, DataModelId.load(data_model)

    def _are_equal(
        self, local: TransformationWrite, cdf_resource: Transformation, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        local_dumped.pop("destinationOidcCredentials", None)
        local_dumped.pop("sourceOidcCredentials", None)
        cdf_dumped = cdf_resource.as_write().dump()
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            # Dry run
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]

        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    @staticmethod
    def _get_query_file(filepath: Path, transformation_external_id: str | None) -> Path | None:
        query_file = filepath.parent / f"{filepath.stem}.sql"
        if not query_file.exists() and transformation_external_id:
            found_query_file = next(
                (
                    f
                    for f in filepath.parent.iterdir()
                    if f.is_file() and f.name.endswith(f"{transformation_external_id}.sql")
                ),
                None,
            )
            if found_query_file is None:
                return None
            query_file = found_query_file
        return query_file

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> TransformationWrite | TransformationWriteList:
        resources = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        # The `authentication` key is custom for this template:

        if isinstance(resources, dict):
            resources = [resources]

        transformations = TransformationWriteList([])

        for resource in resources:
            invalid_parameters: dict[str, str] = {}
            if "action" in resource and "conflictMode" not in resource:
                invalid_parameters["action"] = "conflictMode"
            if "shared" in resource and "isPublic" not in resource:
                invalid_parameters["shared"] = "isPublic"
            if invalid_parameters:
                raise ToolkitInvalidParameterNameError(
                    "Parameters invalid. These are specific for the "
                    "'transformation-cli' and not supported by cognite-toolkit",
                    resource.get("externalId", "<Missing>"),
                    invalid_parameters,
                )

            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id, skip_validation, action="replace dataSetExternalId with dataSetId in transformation"
                )
            if resource.get("conflictMode") is None:
                # Todo; Bug SDK missing default value
                resource["conflictMode"] = "upsert"

            source_oidc_credentials = (
                resource.get("authentication", {}).get("read") or resource.get("authentication") or None
            )
            destination_oidc_credentials = (
                resource.get("authentication", {}).get("write") or resource.get("authentication") or None
            )
            transformation = TransformationWrite.load(resource)
            try:
                transformation.source_oidc_credentials = source_oidc_credentials and OidcCredentials.load(
                    source_oidc_credentials
                )

                transformation.destination_oidc_credentials = destination_oidc_credentials and OidcCredentials.load(
                    destination_oidc_credentials
                )
            except KeyError as e:
                raise ToolkitYAMLFormatError("authentication property is missing required fields", filepath, e)

            query_file = self._get_query_file(filepath, transformation.external_id)

            if transformation.query is None:
                if query_file is None:
                    raise ToolkitYAMLFormatError(
                        f"query property or is missing. It can be inline or a separate file named {filepath.stem}.sql or {transformation.external_id}.sql",
                        filepath,
                    )
                transformation.query = safe_read(query_file)
            elif transformation.query is not None and query_file is not None:
                raise ToolkitYAMLFormatError(
                    f"query property is ambiguously defined in both the yaml file and a separate file named {query_file}\n"
                    f"Please remove one of the definitions, either the query property in {filepath} or the file {query_file}",
                )

            transformations.append(transformation)

        if len(transformations) == 1:
            return transformations[0]
        else:
            return transformations

    def dump_resource(
        self, resource: TransformationWrite, source_file: Path, local_resource: TransformationWrite
    ) -> tuple[dict[str, Any], dict[Path, str]]:
        dumped = resource.dump()
        query = dumped.pop("query")
        dumped.pop("dataSetId", None)
        dumped.pop("sourceOidcCredentials", None)
        dumped.pop("destinationOidcCredentials", None)
        return dumped, {source_file.parent / f"{source_file.stem}.sql": query}

    def create(self, items: Sequence[TransformationWrite]) -> TransformationList:
        return self.client.transformations.create(items)

    def retrieve(self, ids: SequenceNotStr[str]) -> TransformationList:
        return self.client.transformations.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: TransformationWriteList) -> TransformationList:
        return self.client.transformations.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        existing = self.retrieve(ids).as_external_ids()
        if existing:
            self.client.transformations.delete(external_id=existing, ignore_unknown_ids=True)
        return len(existing)

    def iterate(self) -> Iterable[Transformation]:
        return iter(self.client.transformations)

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        spec.update(
            ParameterSpecSet(
                {
                    # Added by toolkit
                    ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=False),
                    ParameterSpec(("authentication",), frozenset({"dict"}), is_required=False, _is_nullable=False),
                    ParameterSpec(
                        ("authentication", "clientId"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "clientSecret"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "scopes"), frozenset({"str"}), is_required=False, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "scopes", ANY_INT), frozenset({"str"}), is_required=False, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "tokenUri"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "cdfProjectName"), frozenset({"str"}), is_required=True, _is_nullable=False
                    ),
                    ParameterSpec(
                        ("authentication", "audience"), frozenset({"str"}), is_required=False, _is_nullable=False
                    ),
                }
            )
        )
        return spec


@final
class TransformationScheduleLoader(
    ResourceLoader[
        str,
        TransformationScheduleWrite,
        TransformationSchedule,
        TransformationScheduleWriteList,
        TransformationScheduleList,
    ]
):
    folder_name = "transformations"
    # Matches all yaml files whose stem contains *schedule or *TransformationSchedule.
    filename_pattern = r"^.*schedule$"
    resource_cls = TransformationSchedule
    resource_write_cls = TransformationScheduleWrite
    list_cls = TransformationScheduleList
    list_write_cls = TransformationScheduleWriteList
    kind = "Schedule"
    dependencies = frozenset({TransformationLoader})
    _doc_url = "Transformation-Schedules/operation/createTransformationSchedules"

    @property
    def display_name(self) -> str:
        return "transformation.schedules"

    @classmethod
    def get_required_capability(cls, items: TransformationScheduleWriteList | None) -> list[Capability]:
        # Access for transformations schedules is checked by the transformation that is deployed
        # first, so we don't need to check for any capabilities here.
        return []

    @classmethod
    def get_id(cls, item: TransformationSchedule | TransformationScheduleWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("TransformationSchedule must have external_id set.")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "externalId" in item:
            yield TransformationLoader, item["externalId"]

    def load_resource(
        self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool
    ) -> TransformationScheduleWrite | TransformationScheduleWriteList | None:
        raw = load_yaml_inject_variables(filepath, ToolGlobals.environment_variables())
        if isinstance(raw, dict):
            return TransformationScheduleWrite.load(raw)
        else:
            return TransformationScheduleWriteList.load(raw)

    def create(self, items: Sequence[TransformationScheduleWrite]) -> TransformationScheduleList:
        try:
            return self.client.transformations.schedules.create(list(items))
        except CogniteDuplicatedError as e:
            existing = {external_id for dup in e.duplicated if (external_id := dup.get("externalId", None))}
            print(
                f"  [bold yellow]WARNING:[/] {len(e.duplicated)} transformation schedules already exist(s): {existing}"
            )
            new_items = [item for item in items if item.external_id not in existing]
            return self.client.transformations.schedules.create(new_items)

    def retrieve(self, ids: SequenceNotStr[str]) -> TransformationScheduleList:
        return self.client.transformations.schedules.retrieve_multiple(external_ids=ids, ignore_unknown_ids=True)

    def update(self, items: TransformationScheduleWriteList) -> TransformationScheduleList:
        return self.client.transformations.schedules.update(items, mode="replace")

    def delete(self, ids: str | SequenceNotStr[str] | None) -> int:
        try:
            self.client.transformations.schedules.delete(
                external_id=cast(SequenceNotStr[str], ids), ignore_unknown_ids=False
            )
            return len(cast(SequenceNotStr[str], ids))
        except CogniteNotFoundError as e:
            return len(cast(SequenceNotStr[str], ids)) - len(e.not_found)

    def iterate(self) -> Iterable[TransformationSchedule]:
        return iter(self.client.transformations.schedules)


@final
class TransformationNotificationLoader(
    ResourceLoader[
        str,
        TransformationNotificationWrite,
        TransformationNotification,
        TransformationNotificationWriteList,
        TransformationNotificationList,
    ]
):
    folder_name = "transformations"
    # Matches all yaml files whose stem ends with *Notification.
    filename_pattern = r"^.*Notification$"
    resource_cls = TransformationNotification
    resource_write_cls = TransformationNotificationWrite
    list_cls = TransformationNotificationList
    list_write_cls = TransformationNotificationWriteList
    kind = "Notification"
    dependencies = frozenset({TransformationLoader})
    _doc_url = "Transformation-Notifications/operation/createTransformationNotifications"
    _split_character = "@@@"

    @property
    def display_name(self) -> str:
        return "transformation.notifications"

    @classmethod
    def get_id(cls, item: TransformationNotification | TransformationNotificationWrite | dict) -> str:
        if isinstance(item, dict):
            if missing := tuple(k for k in {"transformationExternalId", "destination"} if k not in item):
                # We need to raise a KeyError with all missing keys to get the correct error message.
                raise KeyError(*missing)
            return f"{item['transformationExternalId']}{cls._split_character}{item['destination']}"

        return f"{item.transformation_external_id}{cls._split_character}{item.destination}"

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        transformation_id, destination = id.split(cls._split_character, maxsplit=1)
        return {
            "transformationExternalId": transformation_id,
            "destination": destination,
        }

    @classmethod
    def get_required_capability(
        cls, items: TransformationNotificationWriteList | None
    ) -> Capability | list[Capability]:
        # Access for transformation notification is checked by the transformation that is deployed
        # first, so we don't need to check for any capabilities here.
        return []

    def create(self, items: TransformationNotificationWriteList) -> TransformationNotificationList:
        return self.client.transformations.notifications.create(items)  # type: ignore[return-value]

    def retrieve(self, ids: SequenceNotStr[str]) -> TransformationNotificationList:
        retrieved = TransformationNotificationList([])
        for id_ in ids:
            try:
                transformation_external_id, destination = id_.rsplit(self._split_character, maxsplit=1)
            except ValueError:
                # This should never happen, and is a bug in the toolkit if it occurs. Creating a nice error message
                # here so that if it does happen, it will be easier to debug.
                raise ValueError(
                    f"Invalid externalId: {id_}. Must be in the format 'transformationExternalId{self._split_character}destination'"
                )
            try:
                result = self.client.transformations.notifications.list(
                    transformation_external_id=transformation_external_id, destination=destination, limit=-1
                )
            except CogniteAPIError:
                # The notification endpoint gives a 500 if the notification does not exist.
                # The issue has been reported to the service team.
                continue
            retrieved.extend(result)
        return retrieved

    def update(self, items: TransformationNotificationWriteList) -> TransformationNotificationList:
        # Note that since a notification is identified by the combination of transformationExternalId and destination,
        # which is the entire object, an update should never happen. However, implementing just in case.
        item_by_id = {self.get_id(item): item for item in items}
        existing = self.retrieve(list(item_by_id.keys()))
        exiting_by_id = {self.get_id(item): item for item in existing}
        create: list[TransformationNotificationWrite] = []
        unchanged: list[str] = []
        delete: list[int] = []
        for id_, item in item_by_id.items():
            existing_item = exiting_by_id.get(id_)
            if existing_item and self._are_equal(item, existing_item):
                unchanged.append(self.get_id(existing_item))
            else:
                create.append(item)
            if existing_item:
                delete.append(cast(int, existing_item.id))
        if delete:
            self.client.transformations.notifications.delete(delete)
        updated_by_id: dict[str, TransformationNotification] = {}
        if create:
            # Bug in SDK
            created = self.client.transformations.notifications.create(create)
            updated_by_id.update({self.get_id(item): item for item in created})
        if unchanged:
            updated_by_id.update({id_: exiting_by_id[id_] for id_ in unchanged})
        return TransformationNotificationList([updated_by_id[id_] for id_ in item_by_id.keys()])

    def delete(self, ids: SequenceNotStr[str]) -> int:
        # Note that it is theoretically possible that more items will be deleted than
        # input ids. This is because TransformationNotifications are identified by an internal id,
        # while the toolkit uses the transformationExternalId + destination as the id. Thus, there could
        # be multiple notifications for the same transformationExternalId + destination.
        if existing := self.retrieve(ids):
            self.client.transformations.notifications.delete([item.id for item in existing])  # type: ignore[misc]
        return len(existing)

    def iterate(self) -> Iterable[TransformationNotification]:
        return iter(self.client.transformations.notifications)

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        """Returns all items that this item requires.

        For example, a TimeSeries requires a DataSet, so this method would return the
        DatasetLoader and identifier of that dataset.
        """
        if "transformationExternalId" in item:
            yield TransformationLoader, item["transformationExternalId"]
