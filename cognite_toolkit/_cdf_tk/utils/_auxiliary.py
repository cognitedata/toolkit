import inspect
from typing import TypeVar

from cognite_toolkit import _version

T_Cls = TypeVar("T_Cls")


def get_concrete_subclasses(base_cls: type[T_Cls]) -> list[type[T_Cls]]:
    """
    Returns a list of all concrete subclasses of the given base class.
    Args:
        base_cls (type[T_Cls]): The base class to find subclasses for.
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
            if not inspect.isabstract(subclass):
                subclasses.append(subclass)
            seen.add(subclass)
            to_check.append(subclass)
    return subclasses


def get_current_toolkit_version() -> str:
    return _version.__version__
