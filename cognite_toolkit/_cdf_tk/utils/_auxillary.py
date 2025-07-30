import inspect
from typing import TypeVar

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
            if not inspect.isabstract(subclass):
                subclasses.append(subclass)
            if subclass not in seen:
                to_check.append(subclass)
    return subclasses
