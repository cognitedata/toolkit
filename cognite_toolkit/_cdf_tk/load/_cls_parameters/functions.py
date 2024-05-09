from __future__ import annotations

import inspect
from typing import Any

from .constants import BASE_TYPES, CONTAINER_TYPES
from .data_classes import ParameterSet, ParameterSpec, ParameterSpecSet, ParameterValue
from .get_type_hints import _TypeHints
from .type_hint import TypeHint


def read_parameter_from_init_type_hints(cls_: type) -> ParameterSpecSet:
    return _read_parameter_from_init_type_hints(cls_, tuple(), set())


def _read_parameter_from_init_type_hints(cls_: type, path: tuple[str | int, ...], seen: set[str]) -> ParameterSpecSet:
    parameter_set = ParameterSpecSet()
    if not hasattr(cls_, "__init__"):
        return parameter_set  # type: ignore[misc]

    classes = _TypeHints.get_concrete_classes(cls_)
    seen.add(cls_.__name__)
    seen.update(cls_.__name__ for cls_ in classes)
    type_hints_by_name = _TypeHints.get_type_hints_by_name(classes)
    parameters = {k: v for cls in classes for k, v in inspect.signature(cls.__init__).parameters.items()}  # type: ignore[misc]

    for name, parameter in parameters.items():
        if name == "self" or parameter.kind in [parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD]:
            continue
        try:
            hint = TypeHint(type_hints_by_name[name])
        except KeyError:
            # Missing type hint
            parameter_set.is_complete = False
            continue
        is_required = parameter.default is inspect.Parameter.empty
        is_nullable = hint.is_nullable
        parameter_set.add(ParameterSpec((*path, name), hint.frozen_types, is_required, is_nullable))
        if hint.is_base_type:
            continue
        # We iterate as we might have union types
        for sub_hint in hint.sub_hints:
            if sub_hint.is_dict_type:
                key, value = sub_hint.container_args
                dict_set = _read_parameter_from_init_type_hints(value, (*path, name), seen.copy())
                parameter_set.update(dict_set)
            if sub_hint.is_list_type:
                item = sub_hint.container_args[0]
                item_hint = TypeHint(item)
                if item_hint.is_base_type:
                    parameter_set.add(ParameterSpec((*path, name, 0), item_hint.frozen_types, is_required, is_nullable))
                elif item_hint.is_union:
                    for subsub_hint in item_hint.sub_hints:
                        if subsub_hint.is_class:
                            cls_set = _read_parameter_from_init_type_hints(
                                subsub_hint.args[0], (*path, name, 0), seen.copy()
                            )
                            parameter_set.update(cls_set)
                        elif subsub_hint.is_dict_type:
                            key, value = subsub_hint.container_args
                            dict_set = _read_parameter_from_init_type_hints(value, (*path, name, 0), seen.copy())
                            parameter_set.update(dict_set)
                        else:
                            raise NotImplementedError()
                elif item.__name__ in seen:
                    parameter_set.add(ParameterSpec((*path, name, 0), frozenset({"dict"}), is_required, is_nullable))
                else:
                    list_set = _read_parameter_from_init_type_hints(sub_hint.args[0], (*path, name, 0), seen.copy())
                    parameter_set.update(list_set)
            elif sub_hint.is_class:
                arg = sub_hint.args[0]
                if arg.__name__ in seen:
                    parameter_set.add(ParameterSpec((*path, name), frozenset({"dict"}), is_required, is_nullable))
                else:
                    cls_set = _read_parameter_from_init_type_hints(arg, (*path, name), seen.copy())
                    parameter_set.update(cls_set)

    return parameter_set


def read_parameters_from_dict(raw: dict) -> ParameterSet[ParameterValue]:
    return _read_parameters_from_raw(raw, tuple())


def _read_parameters_from_raw(raw: dict | list | Any, path: tuple[str | int, ...]) -> ParameterSet[ParameterValue]:
    parameter_set = ParameterSet[ParameterValue]()
    if type(raw).__name__ in BASE_TYPES:
        parameter_set.add(ParameterValue(path, frozenset({type(raw).__name__}), raw))  # type: ignore[arg-type]
        return parameter_set
    if isinstance(raw, list):
        for i, item in enumerate(raw):
            parameter_set.update(_read_parameters_from_raw(item, (*path, i)))
        return parameter_set
    if isinstance(raw, dict):
        for key, value in raw.items():
            type_ = type(value).__name__
            if type_ in BASE_TYPES:
                parameter_set.add(ParameterValue((*path, key), frozenset({type_}), value))
            elif type_ in CONTAINER_TYPES:
                # We cannot include the value type for containers as it is not hashable
                parameter_set.add(ParameterValue((*path, key), frozenset({type_}), None))
            if isinstance(value, dict):
                parameter_set.update(_read_parameters_from_raw(value, (*path, key)))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    parameter_set.update(_read_parameters_from_raw(item, (*path, key, i)))
    return parameter_set
