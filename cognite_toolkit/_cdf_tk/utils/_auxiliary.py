import inspect
import types
from abc import ABC
from collections.abc import Iterable
from typing import Annotated, Any, Literal, TypeVar, Union, get_args, get_origin

from pydantic_core import PydanticUndefined

from cognite_toolkit import _version

T_Cls = TypeVar("T_Cls")


def literal_string_values_from_annotation(annotation: Any) -> list[str]:
    """Collect string literal values from a typing annotation (Literal, unions, Annotated)."""
    if annotation is None:
        return []
    origin = get_origin(annotation)
    args = get_args(annotation)
    if origin is Literal:
        return [str(a) for a in args]
    if origin is Annotated:
        return literal_string_values_from_annotation(args[0])
    if origin is Union or origin is types.UnionType:
        result: list[str] = []
        for arg in args:
            if arg is type(None):
                continue
            result.extend(literal_string_values_from_annotation(arg))
        return result
    return []


def registry_from_model_classes(classes: Iterable[type[Any]], *, type_field: str) -> dict[str, type[Any]]:
    """Map discriminator string to concrete model class for BeforeValidator routing."""
    registry: dict[str, type[Any]] = {}
    for cls_ in classes:
        field_info = cls_.model_fields.get(type_field)
        if field_info is None:
            continue
        default = field_info.default
        if default is not PydanticUndefined:
            registry[str(default)] = cls_
            continue
        for lit in literal_string_values_from_annotation(field_info.annotation):
            registry[lit] = cls_
    return registry


def registry_from_subclasses_with_type_field(
    base_cls: type[Any],
    *,
    type_field: str,
    exclude: tuple[type[Any], ...] = (),
) -> dict[str, type[Any]]:
    excluded = set(exclude)
    classes: list[type[Any]] = [c for c in get_concrete_subclasses(base_cls) if c not in excluded]
    return registry_from_model_classes(classes, type_field=type_field)


def get_concrete_subclasses(base_cls: type[T_Cls], exclude_ABC_base: bool = True) -> list[type[T_Cls]]:
    """
    Returns a list of all concrete subclasses of the given base class.
    Args:
        base_cls (type[T_Cls]): The base class to find subclasses for.
        exclude_ABC_base (bool): If True, excludes subclasses of ABC base classes.
            Defaults to True.
    Returns:
        list[type[T_Cls]]: A list of concrete subclasses of the base class.
    """
    to_check = [base_cls]
    subclasses: list[type[T_Cls]] = []
    seen: set[type[T_Cls]] = {base_cls}
    while to_check:
        current_cls = to_check.pop()
        for subclass in current_cls.__subclasses__():
            if subclass in seen:
                continue
            if (not inspect.isabstract(subclass)) and (not exclude_ABC_base or ABC not in subclass.__bases__):
                subclasses.append(subclass)
            seen.add(subclass)
            to_check.append(subclass)
    return subclasses


def get_current_toolkit_version() -> str:
    return _version.__version__
