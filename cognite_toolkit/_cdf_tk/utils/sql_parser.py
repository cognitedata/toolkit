import importlib.util
from typing import TYPE_CHECKING

from cognite_toolkit._cdf_tk.exceptions import ToolkitMissingDependencyError

if TYPE_CHECKING:
    from sqlparse.sql import Identifier
    from sqlparse.tokens import Token


class SQLParser:
    def __init__(self, query: str, operation: str) -> None:
        self._verify_dependencies(operation)
        self.query = query
        self._seen_sources: set[str] = set()
        self._sources: list[str] = []
        self._is_parsed = False

    @staticmethod
    def _verify_dependencies(operation: str) -> None:
        if importlib.util.find_spec("sqlparse") is None:
            raise ToolkitMissingDependencyError(
                f"{operation} requires sqlparse. Install with 'pip install \"cognite-toolkit[profile]\"'"
            )

    @property
    def sources(self) -> list[str]:
        """Returns a list of sources (tables) found in the SQL query."""
        if not self._is_parsed:
            self.parse()
            self._is_parsed = True
        return self._sources

    def parse(self) -> None:
        """Parse the SQL query and extract table names."""
        import sqlparse

        parsed = sqlparse.parse(self.query)
        if not parsed:
            return

        for statement in parsed:
            self._find_tables(statement.tokens)
        return

    def _find_tables(self, tokens: "list[Token]") -> None:
        from sqlparse.sql import Identifier, IdentifierList, TokenList
        from sqlparse.tokens import Keyword

        from_seen = False
        for token in tokens:
            if isinstance(token, TokenList):
                # Recursive search nested tokens
                self._find_tables(token.tokens)
            if from_seen:
                if isinstance(token, IdentifierList):
                    self._add_to_sources(*token.get_identifiers())
                elif isinstance(token, Identifier):
                    self._add_to_sources(token)
                elif token.ttype is Keyword:
                    break
            if token.ttype is Keyword and token.value.upper() == "FROM":
                from_seen = True

    def _add_to_sources(self, *sources: "Identifier") -> None:
        """Add a source to the list if it hasn't been seen before."""
        from sqlparse.sql import Identifier
        from sqlparse.tokens import Keyword, Punctuation

        for source in sources:
            if len(source.tokens) < 3 and source[0].ttype is not Punctuation:
                # This is not a source. It should have three tokens:
                # schema, punctuation, and table name.
                continue
            if len(source.tokens) >= 5 and (source[4].ttype is Keyword and source[4].value.upper() == "AS"):
                # Skip the alias part if it exists
                source = Identifier(source.tokens[:3])
            source_str = str(source)
            if source_str not in self._seen_sources:
                self._seen_sources.add(source_str)
                self._sources.append(source_str)
