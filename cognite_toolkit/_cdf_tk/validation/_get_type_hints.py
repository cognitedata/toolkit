from __future__ import annotations

import importlib
import inspect
import typing
from collections import Counter
from typing import Any, get_type_hints

from cognite.client.data_classes._base import CogniteObject


class _TypeHints:
    """
    This class is used to get type hints from the init function of a CogniteObject.

    After Python 3.10, type hints are treated as strings, so we need to evaluate them to get the actual type.
    """

    @classmethod
    def get_type_hints_by_name(cls, signature: inspect.Signature, resource_cls: type[CogniteObject]) -> dict[str, Any]:
        """
        Get type hints from the init function of a CogniteObject.

        Args:
            signature: The signature of the init function.
            resource_cls: The resource class to get type hints from.
        """
        try:
            type_hint_by_name = get_type_hints(resource_cls.__init__, localns=cls._type_checking())
        except TypeError:
            # Python 3.10 Type hints cannot be evaluated with get_type_hints,
            # ref https://stackoverflow.com/questions/66006087/how-to-use-typing-get-type-hints-with-pep585-in-python3-8
            resource_module_vars = vars(importlib.import_module(resource_cls.__module__))
            resource_module_vars.update(cls._type_checking())
            type_hint_by_name = cls._get_type_hints_3_10(resource_module_vars, signature, dict(vars(resource_cls)))
        return type_hint_by_name

    @classmethod
    def _type_checking(cls) -> dict[str, Any]:
        """
        When calling the get_type_hints function, it imports the module with the function TYPE_CHECKING is set to False.

        This function takes all the special types used in data classes and returns them as a dictionary so it
        can be used in the local namespaces.
        """
        import numpy as np
        import numpy.typing as npt
        from cognite.client import CogniteClient

        NumpyDatetime64NSArray = npt.NDArray[np.datetime64]
        NumpyInt64Array = npt.NDArray[np.int64]
        NumpyFloat64Array = npt.NDArray[np.float64]
        NumpyObjArray = npt.NDArray[np.object_]
        return {
            "CogniteClient": CogniteClient,
            "NumpyDatetime64NSArray": NumpyDatetime64NSArray,
            "NumpyInt64Array": NumpyInt64Array,
            "NumpyFloat64Array": NumpyFloat64Array,
            "NumpyObjArray": NumpyObjArray,
        }

    @classmethod
    def _get_type_hints_3_10(
        cls, resource_module_vars: dict[str, Any], signature310: inspect.Signature, local_vars: dict[str, Any]
    ) -> dict[str, Any]:
        return {
            name: cls._create_type_hint_3_10(parameter.annotation, resource_module_vars, local_vars)
            for name, parameter in signature310.parameters.items()
            if name != "self"
        }

    @classmethod
    def _create_type_hint_3_10(
        cls, annotation: str, resource_module_vars: dict[str, Any], local_vars: dict[str, Any]
    ) -> Any:
        if annotation.endswith(" | None"):
            annotation = annotation[:-7]
        try:
            return eval(annotation, resource_module_vars, local_vars)
        except TypeError:
            # Python 3.10 Type Hint
            return cls._type_hint_3_10_to_8(annotation, resource_module_vars, local_vars)

    @classmethod
    @typing.no_type_check
    def _type_hint_3_10_to_8(
        cls, annotation: str, resource_module_vars: dict[str, Any], local_vars: dict[str, Any]
    ) -> Any:
        if cls._is_vertical_union(annotation):
            alternatives = [
                cls._create_type_hint_3_10(a.strip(), resource_module_vars, local_vars) for a in annotation.split("|")
            ]
            return typing.Union[tuple(alternatives)]
        elif annotation.startswith("dict[") and annotation.endswith("]"):
            if Counter(annotation)[","] > 1:
                key, rest = annotation[5:-1].split(",", 1)
                return dict[key.strip(), cls._create_type_hint_3_10(rest.strip(), resource_module_vars, local_vars)]  # type: ignore[misc]
            key, value = annotation[5:-1].split(",")
            return dict[  # type: ignore[misc]
                cls._create_type_hint_3_10(key.strip(), resource_module_vars, local_vars),
                cls._create_type_hint_3_10(value.strip(), resource_module_vars, local_vars),
            ]
        elif annotation.startswith("Mapping[") and annotation.endswith("]"):
            if Counter(annotation)[","] > 1:
                key, rest = annotation[8:-1].split(",", 1)
                return typing.Mapping[  # type: ignore[misc]
                    key.strip(), cls._create_type_hint_3_10(rest.strip(), resource_module_vars, local_vars)
                ]
            key, value = annotation[8:-1].split(",")
            return typing.Mapping[  # type: ignore[misc]
                cls._create_type_hint_3_10(key.strip(), resource_module_vars, local_vars),
                cls._create_type_hint_3_10(value.strip(), resource_module_vars, local_vars),
            ]
        elif annotation.startswith("Optional[") and annotation.endswith("]"):
            return typing.Optional[cls._create_type_hint_3_10(annotation[9:-1], resource_module_vars, local_vars)]  # type: ignore[misc]
        elif annotation.startswith("list[") and annotation.endswith("]"):
            return list[cls._create_type_hint_3_10(annotation[5:-1], resource_module_vars, local_vars)]  # type: ignore[misc]
        elif annotation.startswith("tuple[") and annotation.endswith("]"):
            return tuple[cls._create_type_hint_3_10(annotation[6:-1], resource_module_vars, local_vars)]  # type: ignore[misc]
        elif annotation.startswith("typing.Sequence[") and annotation.endswith("]"):
            # This is used in the Sequence data class file to avoid name collision
            return typing.Sequence[cls._create_type_hint_3_10(annotation[16:-1], resource_module_vars, local_vars)]  # type: ignore[misc]
        raise NotImplementedError(f"Unsupported conversion of type hint {annotation!r}. {cls._error_msg}")

    @classmethod
    def _is_vertical_union(cls, annotation: str) -> bool:
        if "|" not in annotation:
            return False
        parts = [p.strip() for p in annotation.split("|")]
        for part in parts:
            counts = Counter(part)
            if counts["["] != counts["]"]:
                return False
        return True
