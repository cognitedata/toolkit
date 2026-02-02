from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.request_classes.filters import (
    SimulatorModelRevisionFilter,
    SimulatorModelRoutineFilter,
    SimulatorModelRoutineRevisionFilter,
)
from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model import (
    SimulatorModelRequest,
    SimulatorModelResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model_revision import (
    SimulatorModelRevisionRequest,
    SimulatorModelRevisionResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_routine import (
    SimulatorRoutineRequest,
    SimulatorRoutineResponse,
)
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_routine_revision import (
    SimulatorRoutineRevisionRequest,
    SimulatorRoutineRevisionResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import SimulatorModelYAML
from cognite_toolkit._cdf_tk.resource_classes.simulator_model_revision import SimulatorModelRevisionYAML
from cognite_toolkit._cdf_tk.resource_classes.simulator_routine import SimulatorRoutineYAML
from cognite_toolkit._cdf_tk.resource_classes.simulator_routine_revision import SimulatorRoutineRevisionYAML

from .data_organization import DataSetsCRUD
from .file import CogniteFileCRUD, FileMetadataCRUD


@final
class SimulatorModelCRUD(ResourceCRUD[ExternalId, SimulatorModelRequest, SimulatorModelResponse]):
    folder_name = "simulators"
    resource_cls = SimulatorModelResponse
    resource_write_cls = SimulatorModelRequest
    yaml_cls = SimulatorModelYAML
    kind = "SimulatorModel"
    dependencies = frozenset({DataSetsCRUD})
    _doc_url = "Simulator-Models/operation/create_simulator_model_simulators_models_post"

    @property
    def display_name(self) -> str:
        return "simulator models"

    @classmethod
    def get_id(cls, item: SimulatorModelRequest | SimulatorModelResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise KeyError("SimulatorModel must have external_id")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def get_internal_id(cls, item: SimulatorModelResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if not item.id:
            raise KeyError("SimulatorModel must have id")
        return item.id

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SimulatorModelRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        # Simulator ACLs is not yet implemented in the PySDK, which means
        # that we cannot check for specific capabilities.
        return []

    def create(self, items: Sequence[SimulatorModelRequest]) -> list[SimulatorModelResponse]:
        return self.client.tool.simulators.models.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[SimulatorModelResponse]:
        return self.client.tool.simulators.models.retrieve(list(ids), ignore_unknown_ids=True)

    def update(self, items: Sequence[SimulatorModelRequest]) -> list[SimulatorModelResponse]:
        return self.client.tool.simulators.models.update(items, mode="replace")

    def delete(self, ids: SequenceNotStr[ExternalId | InternalOrExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.simulators.models.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[SimulatorModelResponse]:
        # Note: The SimulatorModelsAPI doesn't support data_set_external_id filtering directly,
        # so we iterate and filter in memory if needed.
        cursor: str | None = None
        data_set_id: int | None = None
        if data_set_external_id:
            data_set_id = self.client.lookup.data_sets.id(data_set_external_id, is_dry_run=False)
        while True:
            page = self.client.tool.simulators.models.paginate(
                limit=1000,
                cursor=cursor,
            )
            if data_set_id:
                # Filter by data_set_external_id in memory
                for item in page.items:
                    if item.data_set_id == data_set_id:
                        yield item
            else:
                yield from page.items
            if not page.next_cursor or not page.items:
                break
            cursor = page.next_cursor

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        """Returns all items that this item requires.

        For example, a SimulatorModel requires a DataSet, so this method would return the
        DataSetsCRUD and identifier of that dataset.
        """
        if "dataSetExternalId" in item:
            yield DataSetsCRUD, item["dataSetExternalId"]

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SimulatorModelRequest:
        if ds_external_id := resource.pop("dataSetExternalId", None):
            resource["dataSetId"] = self.client.lookup.data_sets.id(ds_external_id, is_dry_run)
        return SimulatorModelRequest.model_validate(resource)

    def dump_resource(self, resource: SimulatorModelResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        if data_set_id := dumped.pop("dataSetId", None):
            dumped["dataSetExternalId"] = self.client.lookup.data_sets.external_id(data_set_id)
        return dumped


@final
class SimulatorModelRevisionCRUD(
    ResourceCRUD[ExternalId, SimulatorModelRevisionRequest, SimulatorModelRevisionResponse]
):
    folder_name = "simulators"
    resource_cls = SimulatorModelRevisionResponse
    resource_write_cls = SimulatorModelRevisionRequest
    yaml_cls = SimulatorModelRevisionYAML
    kind = "SimulatorModelRevision"
    dependencies = frozenset({SimulatorModelCRUD, CogniteFileCRUD, FileMetadataCRUD})
    _doc_url = "Simulator-Models/operation/create_simulator_model_revision_simulators_models_revisions_post"

    support_update = False

    @property
    def display_name(self) -> str:
        return "simulator model revisions"

    @classmethod
    def get_id(cls, item: SimulatorModelRevisionRequest | SimulatorModelRevisionResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise KeyError("SimulatorModelRevision must have external_id")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def get_internal_id(cls, item: SimulatorModelRevisionResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if not item.id:
            raise KeyError("SimulatorModelRevision must have id")
        return item.id

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SimulatorModelRevisionRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        return []

    def create(self, items: Sequence[SimulatorModelRevisionRequest]) -> list[SimulatorModelRevisionResponse]:
        return self.client.tool.simulators.model_revisions.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[SimulatorModelRevisionResponse]:
        return self.client.tool.simulators.model_revisions.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: SequenceNotStr[ExternalId | InternalOrExternalId]) -> int:
        # Simulator model revisions do not support delete
        raise NotImplementedError("Simulator model revisions do not support delete")

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[SimulatorModelRevisionResponse]:
        model_external_ids: list[str] | None = None
        if parent_ids:
            model_external_ids = [str(pid) for pid in parent_ids]
        for items in self.client.tool.simulators.model_revisions.iterate(
            filter=SimulatorModelRevisionFilter(model_external_ids=model_external_ids)
        ):
            yield from items

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "modelExternalId" in item:
            yield SimulatorModelCRUD, ExternalId(external_id=item["modelExternalId"])
        if "fileExternalId" in item:
            yield FileMetadataCRUD, ExternalId(external_id=item["fileExternalId"])

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SimulatorModelRevisionRequest:
        if file_external_id := resource.pop("fileExternalId", None):
            resource["fileId"] = self.client.lookup.files.id(file_external_id, is_dry_run)
        return SimulatorModelRevisionRequest.model_validate(resource)

    def dump_resource(
        self, resource: SimulatorModelRevisionResponse, local: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        dumped = resource.as_request_resource().dump()
        if file_id := dumped.pop("fileId", None):
            dumped["fileExternalId"] = self.client.lookup.files.external_id(file_id)
        return dumped


@final
class SimulatorRoutineCRUD(ResourceCRUD[ExternalId, SimulatorRoutineRequest, SimulatorRoutineResponse]):
    folder_name = "simulators"
    resource_cls = SimulatorRoutineResponse
    resource_write_cls = SimulatorRoutineRequest
    yaml_cls = SimulatorRoutineYAML
    kind = "SimulatorRoutine"
    dependencies = frozenset({SimulatorModelCRUD})
    _doc_url = "Simulator-Routines/operation/create_simulator_routine_simulators_routines_post"

    support_update = False

    @property
    def display_name(self) -> str:
        return "simulator routines"

    @classmethod
    def get_id(cls, item: SimulatorRoutineRequest | SimulatorRoutineResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise KeyError("SimulatorRoutine must have external_id")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def get_internal_id(cls, item: SimulatorRoutineResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if not item.id:
            raise KeyError("SimulatorRoutine must have id")
        return item.id

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SimulatorRoutineRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        return []

    def create(self, items: Sequence[SimulatorRoutineRequest]) -> list[SimulatorRoutineResponse]:
        return self.client.tool.simulators.routines.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[SimulatorRoutineResponse]:
        # Simulator routines do not have a retrieve endpoint, we need to list and filter
        all_items: list[SimulatorRoutineResponse] = []
        id_set = {id_.external_id for id_ in ids}
        for batch in self.client.tool.simulators.routines.iterate(limit=None):
            for item in batch:
                if item.external_id in id_set:
                    all_items.append(item)
        return all_items

    def delete(self, ids: SequenceNotStr[ExternalId | InternalOrExternalId]) -> int:
        if not ids:
            return 0
        self.client.tool.simulators.routines.delete(list(ids))
        return len(ids)

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[SimulatorRoutineResponse]:
        model_external_ids: list[str] | None = None
        if parent_ids:
            model_external_ids = [str(pid) for pid in parent_ids]
        for items in self.client.tool.simulators.routines.iterate(
            filter=SimulatorModelRoutineFilter(model_external_ids=model_external_ids),
        ):
            yield from items

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "modelExternalId" in item:
            yield SimulatorModelCRUD, ExternalId(external_id=item["modelExternalId"])

    def load_resource(self, resource: dict[str, Any], is_dry_run: bool = False) -> SimulatorRoutineRequest:
        return SimulatorRoutineRequest.model_validate(resource)

    def dump_resource(self, resource: SimulatorRoutineResponse, local: dict[str, Any] | None = None) -> dict[str, Any]:
        return resource.as_request_resource().dump()


@final
class SimulatorRoutineRevisionCRUD(
    ResourceCRUD[ExternalId, SimulatorRoutineRevisionRequest, SimulatorRoutineRevisionResponse]
):
    folder_name = "simulators"
    resource_cls = SimulatorRoutineRevisionResponse
    resource_write_cls = SimulatorRoutineRevisionRequest
    yaml_cls = SimulatorRoutineRevisionYAML
    kind = "SimulatorRoutineRevision"
    dependencies = frozenset({SimulatorRoutineCRUD})
    _doc_url = "Simulator-Routines/operation/create_simulator_routine_revision_simulators_routines_revisions_post"

    support_update = False

    @property
    def display_name(self) -> str:
        return "simulator routine revisions"

    @classmethod
    def get_id(cls, item: SimulatorRoutineRevisionRequest | SimulatorRoutineRevisionResponse | dict) -> ExternalId:
        if isinstance(item, dict):
            return ExternalId(external_id=item["externalId"])
        if not item.external_id:
            raise KeyError("SimulatorRoutineRevision must have external_id")
        return ExternalId(external_id=item.external_id)

    @classmethod
    def get_internal_id(cls, item: SimulatorRoutineRevisionResponse | dict) -> int:
        if isinstance(item, dict):
            return item["id"]
        if not item.id:
            raise KeyError("SimulatorRoutineRevision must have id")
        return item.id

    @classmethod
    def dump_id(cls, id: ExternalId) -> dict[str, Any]:
        return id.dump()

    @classmethod
    def get_required_capability(
        cls, items: Sequence[SimulatorRoutineRevisionRequest] | None, read_only: bool
    ) -> Capability | list[Capability]:
        return []

    def create(self, items: Sequence[SimulatorRoutineRevisionRequest]) -> list[SimulatorRoutineRevisionResponse]:
        return self.client.tool.simulators.routine_revisions.create(items)

    def retrieve(self, ids: SequenceNotStr[ExternalId]) -> list[SimulatorRoutineRevisionResponse]:
        return self.client.tool.simulators.routine_revisions.retrieve(list(ids), ignore_unknown_ids=True)

    def delete(self, ids: SequenceNotStr[ExternalId | InternalOrExternalId]) -> int:
        # Simulator routine revisions do not support delete
        raise NotImplementedError("Simulator routine revisions do not support delete")

    def _iterate(
        self,
        data_set_external_id: str | None = None,
        space: str | None = None,
        parent_ids: list[Hashable] | None = None,
    ) -> Iterable[SimulatorRoutineRevisionResponse]:
        routine_external_ids: list[str] | None = None
        if parent_ids:
            routine_external_ids = [str(pid) for pid in parent_ids]
        for items in self.client.tool.simulators.routine_revisions.iterate(
            filter=SimulatorModelRoutineRevisionFilter(routine_external_ids=routine_external_ids)
        ):
            yield from items

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceCRUD], Hashable]]:
        if "routineExternalId" in item:
            yield SimulatorRoutineCRUD, ExternalId(external_id=item["routineExternalId"])
