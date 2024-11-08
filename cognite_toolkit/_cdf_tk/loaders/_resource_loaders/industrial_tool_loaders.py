from collections.abc import Hashable, Iterable
from functools import lru_cache
from pathlib import Path
from typing import Any, final

from cognite.client.data_classes.capabilities import (
    Capability,
    FilesAcl,
)
from cognite.client.utils.useful_types import SequenceNotStr

from cognite_toolkit._cdf_tk._parameters import ParameterSpec, ParameterSpecSet
from cognite_toolkit._cdf_tk.client.data_classes.streamlit_ import (
    Streamlit,
    StreamlitList,
    StreamlitWrite,
    StreamlitWriteList,
)
from cognite_toolkit._cdf_tk.exceptions import (
    ToolkitRequiredValueError,
)
from cognite_toolkit._cdf_tk.loaders._base_loaders import ResourceLoader
from cognite_toolkit._cdf_tk.utils import (
    CDFToolConfig,
    load_yaml_inject_variables,
)

from .auth_loaders import GroupAllScopedLoader
from .data_organization_loaders import DataSetsLoader


@final
class StreamlitLoader(ResourceLoader[str, StreamlitWrite, Streamlit, StreamlitWriteList, StreamlitList]):
    folder_name = "streamlit"
    filename_pattern = r".*streamlit$"
    resource_cls = Streamlit
    resource_write_cls = StreamlitWrite
    list_cls = StreamlitList
    list_write_cls = StreamlitWriteList
    kind = "Streamlit"
    dependencies = frozenset({DataSetsLoader, GroupAllScopedLoader})
    _doc_url = "Files/operation/initFileUpload"

    @classmethod
    def get_required_capability(
        cls, items: StreamlitWriteList | None, read_only: bool
    ) -> Capability | list[Capability]:
        if not items and items is not None:
            return []

        actions = [FilesAcl.Action.Read] if read_only else [FilesAcl.Action.Read, FilesAcl.Action.Write]

        scope: FilesAcl.Scope.All | FilesAcl.Scope.DataSet = FilesAcl.Scope.All()  # type: ignore[valid-type]
        if items:
            if data_set_ids := {item.data_set_id for item in items if item.data_set_id}:
                scope = FilesAcl.Scope.DataSet(list(data_set_ids))

        return FilesAcl(actions, scope)  # type: ignore[arg-type]

    @classmethod
    def get_id(cls, item: Streamlit | StreamlitWrite | dict) -> str:
        if isinstance(item, dict):
            return item["externalId"]
        if item.external_id is None:
            raise ToolkitRequiredValueError("TimeSeries must have external_id set.")
        return item.external_id

    @classmethod
    def dump_id(cls, id: str) -> dict[str, Any]:
        return {"externalId": id}

    @classmethod
    def get_dependent_items(cls, item: dict) -> Iterable[tuple[type[ResourceLoader], Hashable]]:
        if "dataSetExternalId" in item:
            yield DataSetsLoader, item["dataSetExternalId"]

    def load_resource(self, filepath: Path, ToolGlobals: CDFToolConfig, skip_validation: bool) -> StreamlitWriteList:
        use_environment_variables = (
            ToolGlobals.environment_variables() if self.do_environment_variable_injection else {}
        )
        resources = load_yaml_inject_variables(filepath, use_environment_variables)

        if not isinstance(resources, list):
            resources = [resources]
        for resource in resources:
            if resource.get("dataSetExternalId") is not None:
                ds_external_id = resource.pop("dataSetExternalId")
                resource["dataSetId"] = ToolGlobals.verify_dataset(
                    ds_external_id, skip_validation, action="replace dataSetExternalId with dataSetId in streamlit"
                )
        return StreamlitWriteList._load(resources)

    def _are_equal(
        self, local: StreamlitWrite, cdf_resource: Streamlit, return_dumped: bool = False
    ) -> bool | tuple[bool, dict[str, Any], dict[str, Any]]:
        local_dumped = local.dump()
        cdf_dumped = cdf_resource.as_write().dump()

        # If dataSetId is not set in the local, but are set in the CDF, it is a dry run
        # and we assume they are the same.
        if local_dumped.get("dataSetId") == -1 and "dataSetId" in cdf_dumped:
            local_dumped["dataSetId"] = cdf_dumped["dataSetId"]
        return self._return_are_equal(local_dumped, cdf_dumped, return_dumped)

    def create(self, items: StreamlitWriteList) -> StreamlitList:
        raise NotImplementedError("Streamlit creation is not supported")

    def retrieve(self, ids: SequenceNotStr[str]) -> StreamlitList:
        raise NotImplementedError("Streamlit retrieval is not supported")

    def update(self, items: StreamlitWriteList) -> StreamlitList:
        raise NotImplementedError("Streamlit update is not supported")

    def delete(self, ids: SequenceNotStr[str]) -> int:
        raise NotImplementedError("Streamlit deletion is not supported")

    def iterate(self) -> Iterable[Streamlit]:
        raise NotImplementedError("Streamlit iteration is not supported")

    @classmethod
    @lru_cache(maxsize=1)
    def get_write_cls_parameter_spec(cls) -> ParameterSpecSet:
        spec = super().get_write_cls_parameter_spec()
        # Added by toolkit
        spec.add(ParameterSpec(("dataSetExternalId",), frozenset({"str"}), is_required=False, _is_nullable=True))
        spec.discard(ParameterSpec(("dataSetId",), frozenset({"int"}), is_required=False, _is_nullable=True))
        return spec
