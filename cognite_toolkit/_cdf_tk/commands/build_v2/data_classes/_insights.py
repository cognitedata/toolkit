import csv
import io
import json
from collections import UserList, defaultdict
from typing import ClassVar, Literal, TypeAlias

from pydantic import BaseModel, Field, field_validator

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.utils.file import format_insight_source_file


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
    def display_source_files(self) -> str:
        unique_paths = list(dict.fromkeys([format_insight_source_file(file) for file in self.source_files]))
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
            writer.writerow(
                {
                    "insight_type": _normalize_csv_cell(insight.insight_type()),
                    "code": _normalize_csv_cell(insight.code or ""),
                    "source_file": _normalize_csv_cell(insight.display_source_files),
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
                "sourceFile": insight.display_source_files,
                "message": insight.message,
                "fix": insight.fix,
            }
            for insight in self.data
        ]
        return json.dumps(rows, indent=2, ensure_ascii=False) + "\n"
