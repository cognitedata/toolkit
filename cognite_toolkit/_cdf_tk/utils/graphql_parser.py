import re
import typing
from abc import abstractmethod
from collections.abc import MutableSequence
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Literal

from cognite.client.data_classes.data_modeling import DataModelId, ViewId

if TYPE_CHECKING:
    pass


class GraphQLParser:
    _token_pattern = re.compile(r"[\w\n]+|[^\w\s]", flags=re.DOTALL)
    _multi_newline = re.compile(r"\n+")

    def __init__(self, raw: str, data_model_id: DataModelId) -> None:
        # Ensure consistent line endings
        self.raw = raw.replace("\r\n", "\n").replace("\r", "\n")
        self.data_model_id = data_model_id
        self._entities: list[_Entity] | None = None

    def get_views(self, include_version: bool = False) -> set[ViewId]:
        views: set[ViewId] = set()
        for entity in self._get_entities():
            if entity.is_imported:
                continue
            view_directive: _ViewDirective | None = None
            for directive in entity.directives:
                if isinstance(directive, _ViewDirective):
                    view_directive = directive
                    break
            if view_directive:
                views.add(
                    ViewId(
                        view_directive.space or self.data_model_id.space,
                        view_directive.external_id or entity.identifier,
                        version=view_directive.version if include_version else None,
                    )
                )
            else:
                views.add(ViewId(self.data_model_id.space, entity.identifier))
        return views

    def get_dependencies(self, include_version: bool = False) -> set[ViewId | DataModelId]:
        dependencies: set[ViewId | DataModelId] = set()
        for entity in self._get_entities():
            view_directive: _ViewDirective | None = None
            is_dependency = False
            for directive in entity.directives:
                if isinstance(directive, _Import):
                    if directive.data_model:
                        dependencies.add(directive.data_model)
                        break
                    is_dependency = True
                elif isinstance(directive, _ViewDirective):
                    view_directive = directive
            if is_dependency and view_directive:
                dependencies.add(
                    ViewId(
                        view_directive.space or self.data_model_id.space,
                        view_directive.external_id or entity.identifier,
                        version=view_directive.version if include_version else None,
                    )
                )
            elif is_dependency:
                # Todo: Warning Likely invalid directive
                ...
        return dependencies

    def _get_entities(self) -> "list[_Entity]":
        if self._entities is None:
            self._entities = self._parse()
        return self._entities

    def _parse(self) -> "list[_Entity]":
        entities: list[_Entity] = []
        entity: _Entity | None = None
        last_class: Literal["type", "interface"] | None = None

        parentheses: list[str] = []
        directive_tokens: _DirectiveTokens | None = None
        is_directive_start = False
        is_comment = False
        tokens = self._token_pattern.findall(self.raw)
        for no, token in enumerate(tokens):
            if no >= 2 and (tokens[no - 2 : no + 1] == ['"'] * 3 or tokens[no - 2 : no + 1] == ["'"] * 3):
                is_comment = not is_comment
            if is_comment:
                continue

            token = self._multi_newline.sub("\n", token)
            if token != "\n":
                token = token.removesuffix("\n").removeprefix("\n")

            if token in "({[<":
                parentheses.append(token)
            elif token in ")}]>":
                parentheses.pop()

            is_end_of_entity = bool(parentheses and parentheses[0] == "{")
            if entity and is_end_of_entity:
                # End of entity definition
                if directive_tokens and (directive := directive_tokens.create()):
                    entity.directives.append(directive)
                directive_tokens = None
                entities.append(entity)
                entity = None
            elif entity is not None:
                if directive_tokens is not None and (not parentheses or token == "@"):
                    # End of directive
                    if directive := directive_tokens.create():
                        entity.directives.append(directive)
                    directive_tokens = None
                    if token == "@":
                        is_directive_start = True
                elif directive_tokens:
                    if token == "\n" and "{" not in parentheses:
                        # Throw away.
                        continue
                    # Gather the content of the directive
                    directive_tokens.append(token)
                elif token == "@":
                    is_directive_start = True
                elif is_directive_start and token in ("import", "view"):
                    directive_tokens = _DirectiveTokens([token])
                    is_directive_start = False
                elif is_directive_start:
                    # Not a directive we care about
                    is_directive_start = False

            elif token in ("type", "interface"):
                # Next token starts a new entity definition
                last_class = token
            elif last_class is not None and token != "\n":
                # Start of a new entity definition
                entity = _Entity(identifier=token, class_=last_class)
                last_class = None
        return entities


class _DirectiveTokens(list, MutableSequence[str]):
    def create(self) -> "_Directive | None":
        return _Directive.load(self)


@dataclass
class _Directive:
    # This pattern ignores commas inside }
    SPLIT_ON_COMMA_PATTERN = re.compile(r",(?![^{]*\})")

    @classmethod
    def load(cls, content: list[str]) -> "_Directive | None":
        key, *content = content
        raw_string = "".join(content).removeprefix("(").removesuffix(")").replace("\n", ",")
        data = typing.cast(dict[str, Any], cls._create_args(raw_string))
        if key == "import":
            return _Import._load(data)
        if key == "view":
            return _ViewDirective._load(data)
        return None

    @classmethod
    def _create_args(cls, string: str) -> dict[str, Any] | str:
        if "," not in string and ":" not in string:
            return string
        output: dict[str, Any] = {}
        if string[0] == "{" and string[-1] == "}":
            string = string[1:-1]
        if string[0] == ",":
            string = string[1:]
        if string[-1] == ",":
            string = string[:-1]
        for pair in cls.SPLIT_ON_COMMA_PATTERN.split(string):
            stripped = pair.strip()
            if not stripped or ":" not in stripped:
                continue
            key, value = cls._clean(*stripped.split(":", maxsplit=1))
            output[key] = cls._create_args(value)
        return output

    @classmethod
    def _clean(cls, *args: Any) -> Any:
        return tuple(arg.removeprefix('"').removesuffix('"').removeprefix('"').removesuffix('"') for arg in args)

    @classmethod
    @abstractmethod
    def _load(cls, data: dict[str, Any]) -> "_Directive": ...


@dataclass
class _ViewDirective(_Directive):
    space: str | None = None
    external_id: str | None = None
    version: str | None = None

    @classmethod
    def _load(cls, data: dict[str, Any]) -> "_ViewDirective":
        return _ViewDirective(space=data.get("space"), external_id=data.get("externalId"), version=data.get("version"))


@dataclass
class _Import(_Directive):
    data_model: DataModelId | None = None

    @classmethod
    def _load(cls, data: dict[str, Any]) -> "_Import":
        if "dataModel" in data:
            return _Import(data_model=DataModelId.load(data["dataModel"]))
        return _Import()


@dataclass
class _Entity:
    identifier: str
    class_: Literal["type", "interface"]
    directives: list[_Directive] = field(default_factory=list)

    @property
    def is_imported(self) -> bool:
        return any(isinstance(directive, _Import) for directive in self.directives)
