import csv
import io
import json
from collections import UserList, defaultdict
from collections.abc import Mapping
from pathlib import Path
from typing import Any, ClassVar, Literal, TypeAlias, get_args

from pydantic import BaseModel, Field, ValidationError, field_validator

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.utils.file import format_insight_source_file, relative_to_modules

PATH_SEP_CSV = " | "  # Separator for multiple source files in CSV output
PATH_SEP_JSON = ", "  # Separator for multiple source files in JSON output


class InsightDefinition(BaseModel):
    """Base class for all insights"""

    severity: ClassVar[int] = 999

    message: str
    code: str
    source_files: list[AbsoluteFilePath] = Field(min_length=1)
    fix: str | None = None
    alpha: bool = False

    @property
    def source_file(self) -> AbsoluteFilePath:
        """Return the first source file if multiple are present."""
        return self.source_files[0]

    @classmethod
    def insight_type(cls) -> str:
        return cls.__name__

    @property
    def display_source_files_cwd(self) -> str:
        """Returns a comma-separated string of unique source file paths relative to the current working directory."""
        unique_paths = list(dict.fromkeys([format_insight_source_file(file) for file in self.source_files]))
        return ", ".join(unique_paths)

    @property
    def display_source_files_modules(self) -> str:
        """Returns a comma-separated string of unique source file paths relative to the organization's modules directory."""
        unique_paths = list(dict.fromkeys([relative_to_modules(file) for file in self.source_files]))
        return ", ".join(unique_paths)


class FileReadError(InsightDefinition):
    severity = 60


class ModelSyntaxError(InsightDefinition):
    """If any syntax error is found. Stop validation
    and ask user to fix the syntax error first."""

    severity = 40


class ModelSyntaxWarning(InsightDefinition):
    """A non-blocking syntax issue, such as an unrecognized field. The resource is still built and deployed."""

    severity = 15


class ConsistencyError(InsightDefinition):
    """If any consistency error is found, the deployment of the CDF resource will fail."""

    severity = 45


class InternalValidatorException(BaseModel):
    """A validator threw an unexpected exception and could not complete.

    This should never happen in normal operation — it indicates a bug in the validator itself, not
    necessarily an issue with the resource being validated. Treated like a warning: the build can proceed,
    but the affected resource was not fully validated.
    """

    source: str
    message: str
    code: Literal["INTERNAL-VALIDATOR-EXCEPTION"] = "INTERNAL-VALIDATOR-EXCEPTION"
    fix: str | None = (
        "This is an unexpected error in the validator. It does not necessarily indicate an issue with your resource, only that we failed to validate it. Please report this as a bug."
    )

    MAX_MESSAGE_LENGTH: ClassVar[int] = 2000

    @field_validator("message", mode="after")
    @classmethod
    def _truncate_message(cls, message: str) -> str:
        """The wrapped exception's string representation can be arbitrarily long (e.g. a large stack
        dump from a third-party library). Truncate it so a single insight can't blow up the output."""
        if len(message) <= cls.MAX_MESSAGE_LENGTH:
            return message
        return message[: cls.MAX_MESSAGE_LENGTH] + "... (truncated)"


class IgnoredFileWarning(InsightDefinition):
    severity = 20


class Recommendation(InsightDefinition):
    """Best practice recommendation."""

    severity = 10


Insight: TypeAlias = (
    ModelSyntaxError | ModelSyntaxWarning | ConsistencyError | Recommendation | FileReadError | IgnoredFileWarning
)


def _normalize_csv_cell(text: str) -> str:
    """Normalize line breaks so CSV cells stay readable and consistent across platforms."""
    return text.replace("\r\n", "\n").replace("\r", "\n")


def _split_source_files(raw: str, separator: str) -> list[str]:
    """Split a serialized source-file cell into relative (or absolute) path strings."""
    return [part.strip() for part in raw.split(separator) if part.strip()]


class InsightList(UserList[Insight]):
    """A list of insights that can be sorted by type and message."""

    def by_type(self) -> dict[type[Insight], list[Insight]]:
        """Returns a dictionary of insights sorted by their type."""
        result: dict[type[Insight], list[Insight]] = defaultdict(list)
        for insight in self.data:
            insight_type = type(insight)
            if insight_type not in result:
                result[insight_type] = []
            result[insight_type].append(insight)
        return result

    def by_code(self) -> dict[str, list[Insight]]:
        """Returns a dictionary of insights sorted by their code."""
        result: dict[str, list[Insight]] = defaultdict(list)
        for insight in self.data:
            if insight.code is not None:
                result[insight.code].append(insight)
            else:
                result["UNDEFINED"].append(insight)
        return dict(result)

    @property
    def has_model_syntax_errors(self) -> bool:
        """Returns True if there are any model syntax errors in the insights."""
        return any(isinstance(insight, ModelSyntaxError) for insight in self.data)

    @property
    def has_errors(self) -> bool:
        """Returns True if there are any errors (model syntax or consistency) in the insights."""
        return any(isinstance(insight, (ModelSyntaxError, ConsistencyError)) for insight in self.data)

    @property
    def summary(self) -> dict[str, int]:
        """Returns a summary dict with breakdown of insights by type.

        Returns:
            Dict with keys: syntax_errors, consistency_errors, recommendations
        """

        by_type = self.by_type()

        return {insight_type.__name__: len(insights) for insight_type, insights in by_type.items()}

    def to_csv(self) -> str:
        """Returns a CSV formatted string representation of the insights.

        Uses a Unix-style CSV dialect (LF-only record separators, all fields quoted) so
        ``message`` and ``fix`` may contain newlines without corrupting row boundaries.
        Carriage returns inside cells are normalized to LF newlines.

        Returns:
            CSV formatted string with columns: insight_type, code, source_file, message, fix
        """
        output = io.StringIO()
        fieldnames = ["insight_type", "code", "source_file", "message", "fix"]
        writer = csv.DictWriter(output, fieldnames=fieldnames, dialect=csv.unix_dialect)
        writer.writeheader()

        for insight in self.data:
            unique_paths = list(dict.fromkeys([relative_to_modules(file) for file in insight.source_files]))
            writer.writerow(
                {
                    "insight_type": _normalize_csv_cell(insight.insight_type()),
                    "code": _normalize_csv_cell(insight.code or ""),
                    "source_file": _normalize_csv_cell(PATH_SEP_CSV.join(unique_paths)),
                    "message": _normalize_csv_cell(insight.message),
                    "fix": _normalize_csv_cell(insight.fix or ""),
                }
            )

        return output.getvalue()

    def to_json(self) -> str:
        """Returns a JSON array of insight objects with keys insight_type, code, source_file, message, fix."""

        rows = [
            {
                "insightType": insight.insight_type(),
                "code": insight.code,
                "sourceFile": insight.display_source_files_modules,
                "message": insight.message,
                "fix": insight.fix,
            }
            for insight in self.data
        ]
        return json.dumps(rows, indent=2, ensure_ascii=False) + "\n"

    @classmethod
    def from_file(cls, path: Path, organization_dir: Path) -> "InsightList":
        """Load insights from a CSV or JSON file written during build."""
        return cls(
            [insight for row in cls.read_serialized_rows(path) if (insight := cls._to_insight(row, organization_dir))]
        )

    @classmethod
    def from_csv(cls, content: str, organization_dir: Path) -> "InsightList":
        """Load insights from a CSV string produced by ``to_csv``."""
        return cls(
            [insight for row in cls._rows_from_csv(content) if (insight := cls._to_insight(row, organization_dir))]
        )

    @classmethod
    def from_json(cls, content: str, organization_dir: Path) -> "InsightList":
        """Load insights from a JSON string produced by ``to_json``."""
        return cls(
            [insight for row in cls._rows_from_json(content) if (insight := cls._to_insight(row, organization_dir))]
        )

    @classmethod
    def read_serialized_rows(cls, path: Path) -> list[dict[str, Any]]:
        """Read insights from a CSV or JSON file, keeping source files as serialized relative paths."""
        content = path.read_text(encoding="utf-8")
        if path.suffix.lower() == ".json":
            return cls._rows_from_json(content)
        return cls._rows_from_csv(content)

    @classmethod
    def _rows_from_csv(cls, content: str) -> list[dict[str, Any]]:
        reader = csv.DictReader(io.StringIO(content), dialect=csv.unix_dialect)
        return [
            {
                "insight_type": row.get("insight_type") or "",
                "code": row.get("code") or "",
                "source_files": _split_source_files(str(row.get("source_file") or ""), PATH_SEP_CSV),
                "message": row.get("message") or "",
                "fix": row.get("fix") or None,
            }
            for row in reader
        ]

    @classmethod
    def _rows_from_json(cls, content: str) -> list[dict[str, Any]]:
        data = json.loads(content)
        if not isinstance(data, list):
            raise TypeError("Insights JSON must be a list of objects")
        return [
            {
                "insight_type": row.get("insightType") or "",
                "code": row.get("code") or "",
                "source_files": _split_source_files(str(row.get("sourceFile") or ""), PATH_SEP_JSON),
                "message": row.get("message") or "",
                "fix": row.get("fix") or None,
            }
            for row in data
            if isinstance(row, dict)
        ]

    @classmethod
    def _to_insight(cls, row: Mapping[str, Any], organization_dir: Path) -> Insight | None:
        insight_cls_by_name = {insight_cls.__name__: insight_cls for insight_cls in get_args(Insight)}
        insight_cls = insight_cls_by_name.get(str(row.get("insight_type") or ""))
        if insight_cls is None:
            return None
        source_files: list[Path] = []
        for part in row.get("source_files") or []:
            path = Path(part)
            source_files.append(path if path.is_absolute() else organization_dir / path)
        if not source_files:
            return None
        fix = row.get("fix")
        try:
            return insight_cls(
                message=str(row.get("message") or ""),
                code=str(row.get("code") or ""),
                source_files=source_files,
                fix=fix if fix else None,
            )
        except ValidationError:
            return None
