import inspect
from abc import ABC
from typing import TypeVar

from cognite_toolkit import _version

T_Cls = TypeVar("T_Cls")


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
