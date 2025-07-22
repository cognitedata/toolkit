from cognite.client.data_classes.data_modeling.instances import PropertyValueWrite
from cognite.client.data_classes.data_modeling.views import MappedProperty


def convert_to_mapped_property(value: str | int | float | bool, prop: MappedProperty) -> PropertyValueWrite:
    """Convert a string value to the appropriate type based on the provided property type.

    Args:
        value (str | int | float | bool): The value to convert.
        prop (MappedProperty): The property definition that specifies the type to convert to.

    Returns:
        PropertyValueWrite: The converted value as a PropertyValue, or None if is_nullable is True and the value is empty.
    """
    raise NotImplementedError()
