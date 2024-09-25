from __future__ import annotations

import inspect
from typing import Any, Literal, get_args, get_origin

from .constants import ANY_INT, ANY_STR, ANYTHING, BASE_TYPES, CONTAINER_TYPES
from .data_classes import ParameterSet, ParameterSpec, ParameterSpecSet, ParameterValue
from .get_type_hints import _TypeHints
from .type_hint import TypeHint


def read_parameter_from_init_type_hints(cls_: type) -> ParameterSpecSet:
    return ParameterFromInitTypeHints().read(cls_)


class ParameterFromInitTypeHints:
    """Finds the parameters of a class by reading the type hints of the __init__ method."""

    def __init__(self) -> None:
        self.parameter_set = ParameterSpecSet()

    def read(self, cls_: type) -> ParameterSpecSet:
        self._read(cls_, tuple(), set())
        return self.parameter_set

    def _read(self, cls_: type, path: tuple[str | int, ...], seen: set[str]) -> None:
        if not hasattr(cls_, "__init__"):
            return None

        classes = _TypeHints.get_concrete_classes(cls_)
        try:
            seen.add(cls_.__name__)
        except AttributeError:
            # Python 3.9
            if str(cls_) == "typing.Any":
                return None
            raise
        seen.update(cls_.__name__ for cls_ in classes)
        for cls_ in classes:
            type_hints_by_name = _TypeHints.get_type_hints_by_name(cls_)
            for name, parameter in inspect.signature(cls_.__init__).parameters.items():  # type: ignore[misc]
                if name == "self" or parameter.kind in [parameter.VAR_POSITIONAL, parameter.VAR_KEYWORD]:
                    continue
                try:
                    hint = TypeHint(type_hints_by_name[name])
                except KeyError:
                    # Missing type hint
                    self.parameter_set.is_complete = False
                    continue

                is_required = parameter.default is inspect.Parameter.empty
                is_nullable = hint.is_nullable or parameter.default is None
                self.parameter_set.add(ParameterSpec((*path, name), hint.frozen_types, is_required, is_nullable))
                for subhint in hint.sub_hints:
                    if not subhint.is_base_type:
                        self._create_nested_parameters((name,), is_required, hint, path, seen)

    def _create_nested_parameters(
        self,
        parent_name: tuple[str | int, ...],
        is_parent_required: bool,
        hint: TypeHint,
        path: tuple[str | int, ...],
        seen: set[str],
    ) -> None:
        for sub_hint in hint.sub_hints:
            if sub_hint.is_dict_type:
                self._create_parameter_spec_dict(sub_hint, parent_name, path, seen)
            if sub_hint.is_list_type:
                self._create_parameter_spec_list(sub_hint, parent_name, is_parent_required, path, seen)
            if sub_hint.is_user_defined_class:
                self._create_parameter_spec_class(sub_hint, parent_name, is_parent_required, path, seen)
        return None

    def _create_parameter_spec_dict(
        self, hint: TypeHint, parent_name: tuple[str | int, ...], path: tuple[str | int, ...], seen: set[str]
    ) -> None:
        try:
            key, value = hint.container_args
        except ValueError:
            # There are no type hints for the dict
            self.parameter_set.add(
                ParameterSpec(
                    (*path, *parent_name, ANYTHING), frozenset({"unknown"}), is_required=False, _is_nullable=True
                )
            )
            return
        if get_origin(key) is Literal:
            args = get_args(key)
            key_path = args[0]
        elif key is str:
            key_path = ANY_STR
        else:
            raise NotImplementedError(
                f"Only Literal and str are supported for dict keys. {key.__name__} is not supported."
            )
        value_hint = TypeHint(value)

        self.parameter_set.add(
            ParameterSpec(
                (*path, *parent_name, key_path),
                value_hint.frozen_types,
                is_required=False,
                _is_nullable=value_hint.is_nullable,
            )
        )
        for sub_hint in value_hint.sub_hints:
            if not (sub_hint.is_base_type or sub_hint.is_any):
                self._create_nested_parameters(tuple(), False, sub_hint, (*path, *parent_name, ANY_STR), seen.copy())
        # if not (value_hint.is_base_type or value_hint.is_any):
        #     self._read(value, (*path, *parent_name, ANY_STR), seen.copy())

    def _create_parameter_spec_list(
        self,
        hint: TypeHint,
        parent_name: tuple[str | int, ...],
        parent_is_required: bool,
        path: tuple[str | int, ...],
        seen: set[str],
    ) -> None:
        try:
            item = hint.container_args[0]
        except IndexError:
            # There are no type hints for the list
            self.parameter_set.add(
                ParameterSpec(
                    (*path, *parent_name, ANYTHING), frozenset({"unknown"}), is_required=False, _is_nullable=True
                )
            )
            return
        item_hint = TypeHint(item)
        if item_hint.is_base_type:
            self.parameter_set.add(
                ParameterSpec(
                    (*path, *parent_name, ANY_INT),
                    item_hint.frozen_types,
                    is_required=False,
                    _is_nullable=item_hint.is_nullable,
                )
            )
        else:
            self._create_nested_parameters(
                tuple(), parent_is_required, item_hint, (*path, *parent_name, ANY_INT), seen.copy()
            )

    def _create_parameter_spec_class(
        self,
        hint: TypeHint,
        parent_name: tuple[str | int, ...],
        parent_is_required: bool,
        path: tuple[str | int, ...],
        seen: set[str],
    ) -> None:
        cls_ = hint.args[0]
        if cls_.__name__ in seen:
            # This is to avoid infinite recursion
            self.parameter_set.add(
                ParameterSpec(
                    (*path, *parent_name), frozenset({"dict"}), parent_is_required, _is_nullable=hint.is_nullable
                )
            )
        else:
            self._read(cls_, (*path, *parent_name), seen.copy())


def read_parameters_from_dict(raw: dict) -> ParameterSet[ParameterValue]:
    return _read_parameters_from_raw(raw, tuple())


def _read_parameters_from_raw(raw: dict | list | Any, path: tuple[str | int, ...]) -> ParameterSet[ParameterValue]:
    parameter_set = ParameterSet[ParameterValue]()
    if type(raw).__name__ in BASE_TYPES:
        parameter_set.add(ParameterValue(path, type(raw).__name__, raw))  # type: ignore[arg-type]
        return parameter_set
    if isinstance(raw, list):
        for i, item in enumerate(raw):
            parameter_set.update(_read_parameters_from_raw(item, (*path, i)))
        return parameter_set
    if isinstance(raw, dict):
        for key, value in raw.items():
            type_ = type(value).__name__
            if type_ in BASE_TYPES:
                parameter_set.add(ParameterValue((*path, key), type_, value))
            elif type_ in CONTAINER_TYPES:
                # We cannot include the value type for containers as it is not hashable
                parameter_set.add(ParameterValue((*path, key), type_, None))
            if isinstance(value, dict):
                parameter_set.update(_read_parameters_from_raw(value, (*path, key)))
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    parameter_set.update(_read_parameters_from_raw(item, (*path, key, i)))
    return parameter_set
