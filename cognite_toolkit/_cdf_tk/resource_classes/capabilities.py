import sys
from types import MappingProxyType
from typing import Any, ClassVar, Literal, cast

from pydantic import BaseModel, ModelWrapValidatorHandler, field_validator, model_serializer, model_validator
from pydantic_core.core_schema import SerializerFunctionWrapHandler

from cognite_toolkit._cdf_tk.utils.collection import humanize_collection

if sys.version_info < (3, 11):
    from typing_extensions import Self
else:
    from typing import Self

from .base import BaseModelResource


class Scope(BaseModelResource):
    _scope_name: ClassVar[str]

    @model_validator(mode="wrap")
    @classmethod
    def find_scope_cls(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if isinstance(data, Scope):
            return cast(Self, data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid scope data '{type(data)}' expected dict")

        if cls is not Scope:
            return handler(data)
        name, content = next(iter(data.items()))
        if name not in _SCOPE_CLASS_BY_NAME:
            raise ValueError(
                f"Invalid scope name '{name}'. Expected one of {humanize_collection(_SCOPE_CLASS_BY_NAME.keys(), bind_word='or')}"
            )
        cls_ = _SCOPE_CLASS_BY_NAME[name]
        return cast(Self, cls_.model_validate(content))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_scope_name(self, handler: SerializerFunctionWrapHandler) -> dict:
        if self._scope_name is None:
            raise ValueError("Scope name is not set")
        serialized_data = handler(self)
        return {self._scope_name: serialized_data}


class AllScope(Scope):
    _scope_name = "all"


class CurrentUserScope(Scope):
    _scope_name = "currentuserscope"


class IDScope(Scope):
    _scope_name = "idScope"
    ids: list[int]


class IDScopeLowerCase(Scope):
    """Necessary due to lack of API standardisation on scope name: 'idScope' VS 'idscope'"""

    _scope_name = "idscope"
    ids: list[int]


class InstancesScope(Scope):
    _scope_name = "instancesScope"
    instances: list[str]


class ExtractionPipelineScope(Scope):
    _scope_name = "extractionPipelineScope"
    ids: list[int]


class PostgresGatewayUsersScope(Scope):
    _scope_name = "usersScope"
    usernames: list[str]


class DataSetScope(Scope):
    _scope_name = "datasetScope"
    ids: list[int]


class TableScope(Scope):
    _scope_name = "tableScope"
    dbs_to_tables: dict[str, list[str]]


class AssetRootIDScope(Scope):
    _scope_name = "assetRootIdScope"
    root_ids: list[int]


class ExperimentsScope(Scope):
    _scope_name = "experimentscope"
    experiments: list[str]


class SpaceIDScope(Scope):
    _scope_name = "spaceIdScope"
    space_ids: list[str]


class PartitionScope(Scope):
    _scope_name = "partition"
    partition_ids: list[int]


class LegacySpaceScope(Scope):
    _scope_name = "spaceScope"
    external_ids: list[str]


class LegacyDataModelScope(Scope):
    _scope_name = "dataModelScope"
    external_ids: list[str]


class Capability(BaseModel):
    _capability_name: ClassVar[str]

    @model_validator(mode="wrap")
    @classmethod
    def find_capability_cls(cls, data: Any, handler: ModelWrapValidatorHandler[Self]) -> Self:
        if cls is not Capability:
            return handler(data)
        if not isinstance(data, dict):
            raise ValueError(f"Invalid capability data '{type(data)}' expected dict")
        name, content = next(iter(data.items()))
        if name not in _CAPABILITY_CLASS_BY_NAME:
            raise ValueError(f"Invalid capability name '{name}'. Expected one of {_CAPABILITY_CLASS_BY_NAME.keys()}")
        cls_ = _CAPABILITY_CLASS_BY_NAME[name]
        return cast(Self, cls_.model_validate(content))

    @model_serializer(mode="wrap", when_used="always", return_type=dict)
    def include_capability_name(self, handler: SerializerFunctionWrapHandler) -> dict:
        if self._capability_name is None:
            raise ValueError("Capability name is not set")
        serialized_data = handler(self)
        return {self._capability_name: serialized_data}


class DataModelInstancesAcl(Capability):
    _capability_name = "dataModelInstancesAcl"
    actions: list[Literal["READ", "WRITE", "WRITE_PROPERTIES"]]
    scope: AllScope | SpaceIDScope

    @field_validator("scope", mode="before")
    @classmethod
    def find_scope_cls(cls, data: Any) -> Scope:
        return Scope.model_validate(data)


class DataModelsAcl(Capability):
    _capability_name = "dataModelsAcl"
    actions: list[Literal["READ", "WRITE"]]
    scope: AllScope | SpaceIDScope

    @field_validator("scope", mode="before")
    @classmethod
    def find_scope_cls(cls, data: Any) -> Scope:
        return Scope.model_validate(data)


_CAPABILITY_CLASS_BY_NAME: MappingProxyType[str, type[Capability]] = MappingProxyType(
    {c._capability_name: c for c in Capability.__subclasses__()}
)
ALL_CAPABILITIES = sorted(_CAPABILITY_CLASS_BY_NAME)

_SCOPE_CLASS_BY_NAME: MappingProxyType[str, type[Scope]] = MappingProxyType(
    {s._scope_name: s for s in Scope.__subclasses__()}
)
