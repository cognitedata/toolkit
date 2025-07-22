from abc import ABC, abstractmethod
from collections.abc import Mapping
from typing import ClassVar

from cognite.client.data_classes.data_modeling.containers import ContainerId
from cognite.client.data_classes.data_modeling.data_types import ListablePropertyType
from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite
from cognite.client.data_classes.data_modeling.views import MappedProperty

_SPECIAL_CASES = {
    (ContainerId("cdf_cdm", "CogniteTimeSeries"), "type"),
}


def convert_to_primary_property(
    value: str | int | float | bool | dict | list, prop: MappedProperty
) -> PropertyValueWrite:
    """Convert a string value to the appropriate type based on the provided property type.

    Args:
        value (str | int | float | bool): The value to convert.
        prop (MappedProperty): The property definition that specifies the type to convert to.

    Returns:
        PropertyValueWrite: The converted value as a PropertyValue, or None if is_nullable is True and the value is empty.
    """
    converter: type[_Converter]
    if (prop.container, prop.container_property_identifier) in CONVERTER_BY_CONTAINER_PROPERTY:
        converter = CONVERTER_BY_CONTAINER_PROPERTY[(prop.container, prop.container_property_identifier)]
    elif prop.type._type in CONVERTER_BY_TYPE:
        converter = CONVERTER_BY_TYPE[prop.type._type]
    else:
        raise TypeError(f"Unsupported property type {prop.type}")
    if isinstance(prop.type, ListablePropertyType) and prop.type.is_list:
        raise NotImplementedError(f"Listable property type {prop.type} is not supported")
    return converter().convert(value)


class _Converter(ABC):
    @abstractmethod
    def convert(self, value: str | int | float | bool | dict | list) -> PropertyValueWrite:
        """Convert a value to the appropriate type."""
        raise NotImplementedError("This method should be implemented by subclasses.")


class _SpecialCaseConverter(_Converter, ABC):
    """Abstract base class for converters handling special cases."""

    container_property: ClassVar[tuple[ContainerId, str]]


class _ValueConverter(_Converter, ABC):
    type: ClassVar[str]


CONVERTER_BY_TYPE: Mapping[str, type[_ValueConverter]] = {cls_.type: cls_ for cls_ in _ValueConverter.__subclasses__()}  # type: ignore[type-abstract]
CONVERTER_BY_CONTAINER_PROPERTY: Mapping[tuple[ContainerId, str], type[_SpecialCaseConverter]] = {
    cls_.container_property: cls_  # type: ignore[type-abstract]
    for cls_ in _SpecialCaseConverter.__subclasses__()
}
