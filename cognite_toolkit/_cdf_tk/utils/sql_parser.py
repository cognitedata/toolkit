import importlib.util
from typing import TYPE_CHECKING

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingDependencyError

if TYPE_CHECKING:
    from sqlparse.sql import Identifier
    from sqlparse.tokens import Token
from dataclasses import dataclass


@dataclass(frozen=True)
class SQLTable:
    schema: str
    name: str

    def __str__(self) -> str:
        """Return the table name in the format 'schema.table'."""
        return f"{self.schema}.{self.name}"


class SQLParser:
    def __init__(self, query: str, operation: str) -> None:
        self._verify_dependencies(operation)
        self.query = query
        self._seen_sources: set[SQLTable] = set()
        self._sources: list[SQLTable] = []
        self._destination_columns: list[str] = []
        self._is_parsed = False

    @staticmethod
    def _verify_dependencies(operation: str) -> None:
        if importlib.util.find_spec("sqlparse") is None:
            raise ToolkitMissingDependencyError(
                f"{operation} requires sqlparse. Install with 'pip install \"cognite-toolkit[sql]\"'"
            )

    @property
    def sources(self) -> list[SQLTable]:
        """Returns a list of sources (tables) found in the SQL query."""
        if not self._is_parsed:
            self.parse()
        return self._sources

    @property
    def destination_columns(self) -> list[str]:
        """Returns a list of destination columns found in the SQL query."""
        if not self._is_parsed:
            self.parse()
        return self._destination_columns

    def is_using_data_set(
        self, data_set_ids: list[int] | None = None, data_set_external_ids: list[str] | None = None
    ) -> bool:
        """Check if the SQL query uses any of the provided data set IDs or external IDs."""
        for data_set_id in data_set_ids or []:
            if f"{data_set_id} " in self.query:
                return True
        for data_set_external_id in data_set_external_ids or []:
            if f'dataset_id("{data_set_external_id}")' in self.query:
                return True
            elif f"dataset_id('{data_set_external_id}')" in self.query:
                return True
        return False

    def parse(self) -> None:
        """Parse the SQL query and extract table names."""
        import sqlparse

        parsed = sqlparse.parse(self.query)
        if not parsed:
            self._is_parsed = True
            return

        for statement in parsed:
            self._find_tables(statement.tokens)
            self._find_destination_columns(statement.tokens)
        self._is_parsed = True
        return

    def _find_destination_columns(self, tokens: "list[Token]") -> None:
        from sqlparse.sql import Comment, Identifier, IdentifierList
        from sqlparse.tokens import DML, Wildcard

        content_tokens = [
            token
            for token in tokens
            if not token.is_whitespace and not token.is_newline and not isinstance(token, Comment)
        ]
        is_next: bool = False
        for token in content_tokens:
            if is_next and isinstance(token, IdentifierList):
                self._add_destination_columns(*token.get_identifiers())
                break
            elif is_next and isinstance(token, Identifier):
                self._add_destination_columns(token)
            elif is_next and token.ttype is Wildcard and token.normalized == "*":
                self._destination_columns.append("*")
                break
            elif is_next and token.is_keyword:
                break
            if token.ttype is DML and token.normalized == "SELECT":
                is_next = True

    def _add_destination_columns(self, *identifiers: "Identifier") -> None:
        from sqlparse.sql import Comment, Identifier

        for identifier in identifiers:
            # The identifier is either a `column_name` or `some expression AS column_name`.
            # Thus, we simply take the last token in the identifier.
            if isinstance(identifier, Identifier) and identifier.tokens:
                content_tokens = [
                    token
                    for token in identifier.tokens
                    if not token.is_whitespace and not token.is_newline and not isinstance(token, Comment)
                ]
                if content_tokens:
                    self._destination_columns.append(content_tokens[-1].value)

    def _find_tables(self, tokens: "list[Token]") -> None:
        from sqlparse.sql import Identifier, IdentifierList, TokenList
        from sqlparse.tokens import Keyword

        content_tokens = [token for token in tokens if not token.is_whitespace and not token.is_newline]

        is_next_source: bool = False
        for token in content_tokens:
            if is_next_source and isinstance(token, Identifier):
                if not self._add_to_source(token):
                    # This could be a nested expression like '(SELECT ... FROM ...) Identifier'.
                    self._find_tables(token.tokens)
                is_next_source = False
            elif is_next_source and isinstance(token, IdentifierList):
                for identifier in token.get_identifiers():
                    if not self._add_to_source(identifier):
                        self._find_tables(identifier.tokens)
            elif token.ttype is Keyword and (token.normalized == "FROM" or token.normalized.endswith("JOIN")):
                is_next_source = True
            elif isinstance(token, TokenList):
                # If the token is a TokenList, we need to check its tokens recursively.
                self._find_tables(token.tokens)

    def _add_to_source(self, source: "Identifier") -> bool:
        """Add a source to the list if it hasn't been seen before."""
        from sqlparse.tokens import Name

        names = [token for token in source.tokens if token.ttype is Name]

        if len(names) != 2:
            # We are expecting a schema and a table name
            return False
        table = SQLTable(schema=self._clean_name(names[0].value), name=self._clean_name(names[1].value))
        if table not in self._seen_sources:
            self._seen_sources.add(table)
            self._sources.append(table)
        return True

    @staticmethod
    def _clean_name(name: str) -> str:
        """Clean the name by removing quotes and whitespace."""
        return name.strip().removeprefix("`").removesuffix("`").strip()
