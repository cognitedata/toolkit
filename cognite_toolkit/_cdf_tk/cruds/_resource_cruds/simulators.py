from collections.abc import Hashable, Iterable, Sequence
from typing import Any, final

from cognite.client.data_classes.capabilities import Capability
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk.client.resource_classes.identifiers import ExternalId, InternalOrExternalId
from cognite_toolkit._cdf_tk.client.resource_classes.simulator_model import (
    SimulatorModelRequest,
    SimulatorModelResponse,
)
from cognite_toolkit._cdf_tk.cruds._base_cruds import ResourceCRUD
from cognite_toolkit._cdf_tk.resource_classes import SimulatorModelYAML

from .data_organization import DataSetsCRUD


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
