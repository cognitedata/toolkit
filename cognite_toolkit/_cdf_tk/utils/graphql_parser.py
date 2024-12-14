import re
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
        is_multiline_comment = False
        is_end_of_line_comment = False
        is_in_double_quote = False
        is_in_single_quote = False
        tokens = self._token_pattern.findall(self.raw)
        for no, token in enumerate(tokens):
            if no >= 2 and (tokens[no - 2 : no + 1] == ['"'] * 3 or tokens[no - 2 : no + 1] == ["'"] * 3):
                is_multiline_comment = not is_multiline_comment
            elif token == '"':
                is_in_double_quote = not is_in_double_quote
            elif token == "'":
                is_in_single_quote = not is_in_single_quote
            elif token == "#" and not (is_in_double_quote or is_in_single_quote):
                is_end_of_line_comment = True
            if "\n" in token and is_end_of_line_comment:
                is_end_of_line_comment = False
            if is_multiline_comment or is_end_of_line_comment:
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
                    # Gather the content of the directive
                    directive_tokens.append(token)
                elif token == "@":
                    is_directive_start = True
                elif is_directive_start:
                    if token in ("import", "view"):
                        # We only care about import and view directives
                        directive_tokens = _DirectiveTokens([token])
                    is_directive_start = False

            elif token in ("type", "interface") and not parentheses:
                # Next token starts a new entity definition
                # Notet hat we cannot be inside a paranthesis as that could be a
                # property of the entity
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
        raw_string = cls._standardize(content)
        data = cls._create_args(raw_string)
        if isinstance(data, list):
            return None
        if key == "import":
            return _Import._load(data)
        if key == "view":
            return _ViewDirective._load(data)
        return None

    @classmethod
    def _standardize(cls, content: list[str]) -> str:
        """We standardize to use commas as separators, instead of newlines.
        However, if we are inside a parenthesis we need to replace newlines with empty.
        """
        if not content:
            return ""
        # Ensure that the content is wrapped in parenthesis
        # so that we can safely drop the first and last character
        if content[0] != "(":
            content.insert(0, "(")
        if content[-1] != ")":
            content.append(")")

        standardized: list[str] = []
        for last, current, next_ in zip(content, content[1:], content[2:]):
            if current == "\n" and last in ")}]" and next_ in "({[":
                standardized.append(",")
            elif current == "\n" and last in ":({[":
                continue
            elif current == "\n" and next_ in ")}]":
                continue
            elif current == "\n":
                standardized.append(",")
            else:
                standardized.append(current)
        return "".join(standardized)

    @classmethod
    def _create_args(cls, string: str) -> dict[str, Any] | str | list[Any]:
        if "," not in string and ":" not in string:
            return string
        if string[0] == "{" and string[-1] == "}":
            string = string[1:-1]
        is_list = False
        if string[0] == "[" and string[-1] == "]":
            string = string[1:-1]
            is_list = True
        items: list[Any] = []
        obj: dict[str, Any] = {}
        last_pair = ""
        for pair in cls.SPLIT_ON_COMMA_PATTERN.split(string):
            stripped = pair.strip()
            if (not stripped or (not is_list and ":" not in stripped)) and not last_pair:
                continue
            if last_pair:
                stripped = f"{last_pair},{stripped}"
                last_pair = ""
            # Regex does not deal with nested parenthesis
            left_count = sum(stripped.count(char) for char in "{[(")
            right_count = sum(stripped.count(char) for char in "}])")
            if left_count != right_count:
                last_pair = stripped
                continue
            if is_list:
                items.append(cls._create_args(stripped))
            else:
                key, value = cls._clean(*stripped.split(":", maxsplit=1))
                if set("{[(}]}") & set(key):
                    raise ValueError(f"Invalid value {value}")
                obj[key] = cls._create_args(value)
        return items if is_list else obj

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
    def _load(cls, data: dict[str, Any] | str) -> "_ViewDirective":
        if isinstance(data, str):
            return _ViewDirective()
        space = data.get("space")
        external_id = data.get("externalId")
        version = data.get("version")
        for variable in (space, external_id, version):
            if variable and not isinstance(variable, str):
                raise ValueError(f"Invalid variable {variable}")
        return _ViewDirective(space=space, external_id=external_id, version=version)


@dataclass
class _Import(_Directive):
    data_model: DataModelId | None = None

    @classmethod
    def _load(cls, data: dict[str, Any] | str) -> "_Import":
        if isinstance(data, str):
            return _Import()
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
