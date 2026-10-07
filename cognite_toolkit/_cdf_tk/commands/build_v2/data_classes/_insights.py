import csv
import io
import json
import sys
from collections import Counter, UserList, defaultdict
from pathlib import Path
from typing import Annotated, Any, ClassVar, Final, Literal, TypeVar

from pydantic import BaseModel, Field, TypeAdapter, field_serializer, field_validator
from pydantic_core.core_schema import ValidationInfo

from cognite_toolkit._cdf_tk.commands.build_v2.data_classes._types import AbsoluteFilePath
from cognite_toolkit._cdf_tk.constants import BUILD_FOLDER_ENCODING
from cognite_toolkit._cdf_tk.exceptions import ToolkitValidationError
from cognite_toolkit._cdf_tk.feature_flags import Flags
from cognite_toolkit._cdf_tk.utils.file import format_insight_source_file, relative_to_modules

if sys.version_info >= (3, 11):
    from typing import Self
else:
    from typing_extensions import Self

T_Insight = TypeVar("T_Insight", bound="InsightDefinition")

BUILD_ERROR_TYPE: Final = "Error"
BUILD_WARNING_TYPE: Final = "Warning"


class InsightDefinition(BaseModel):
    """Base class for all insights"""

    insight_type: str = "InsightDefinition"
    severity: ClassVar[int] = 999
    title: ClassVar[str | None] = None

    code: str
    message: str
    source_file: AbsoluteFilePath
    line: int | None = None
    column: int | None = None
    fix: str | None = None
    alpha: bool = False

    @property
    def display_source_file_cwd(self) -> str:
        """Returns the source file path relative to the current working directory."""
        return format_insight_source_file(self.source_file)

    @property
    def display_location(self) -> str:
        """The source file, with ':line:column' appended when the position is known.

        The 'path:line:column' format is made clickable by terminals such as the one in VS Code.
        """
        location = self.display_source_file_cwd
        if self.line is None:
            return location
        if self.column is None:
            return f"{location}:{self.line}"
        return f"{location}:{self.line}:{self.column}"

    @property
    def display_source_file_modules(self) -> str:
        """Returns the source file path relative to the organization's modules directory."""
        return relative_to_modules(self.source_file)

    @property
    def heading(self) -> str:
        """A short human-readable title for this insight."""
        if self.title is not None:
            return self.title
        return self.code.replace("-", " ").replace("_", " ").capitalize()

    @field_validator("line", "column", mode="before")
    @classmethod
    def _empty_to_none(cls, value: Any) -> Any:
        """CSV serializes a missing position as an empty cell."""
        return None if value == "" else value

    @field_validator("source_file", mode="before")
    @classmethod
    def _to_absolute_path(cls, value: Any, info: ValidationInfo) -> Path:
        """Convert the source file path to an absolute path relative to the organization directory."""
        if not isinstance(value, str | Path):
            raise ValueError(f"Unexpected type for source_file: {type(value)}")
        if info.context and isinstance(organization_dir := info.context.get("organization_dir", None), Path):
            return (organization_dir / value).resolve()
        return Path(value).resolve()

    @field_serializer("source_file")
    def as_relative_to_modules(self, source_file: AbsoluteFilePath) -> str:
        """Serialize the source_file field as a path relative to the organization's modules directory."""
        return relative_to_modules(source_file)

    @field_validator("message", "fix", mode="after")
    @classmethod
    def normalize_line_breaks(cls, value: str | None) -> str | None:
        """Normalize line breaks to LF-only so values round-trip consistently across formats."""
        if value is None:
            return value
        return _normalize_csv_cell(value)


class FileReadError(InsightDefinition):
    insight_type: Literal["FileReadError"] = "FileReadError"
    severity = 60


class ModelSyntaxError(InsightDefinition):
    """If any syntax error is found. Stop validation
    and ask user to fix the syntax error first."""

    insight_type: Literal["ModelSyntaxError"] = "ModelSyntaxError"
    severity = 40


class ModelSyntaxWarning(InsightDefinition):
    """A non-blocking syntax issue, such as an unrecognized field. The resource is still built and deployed."""

    insight_type: Literal["ModelSyntaxWarning"] = "ModelSyntaxWarning"
    severity = 15


class ConsistencyError(InsightDefinition):
    """If any consistency error is found, the deployment of the CDF resource will fail."""

    insight_type: Literal["ConsistencyError"] = "ConsistencyError"
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
    """A file was ignored because it was not recognized as a valid resource file."""

    insight_type: Literal["IgnoredFileWarning"] = "IgnoredFileWarning"
    severity = 20


class Recommendation(InsightDefinition):
    """Best practice recommendation."""

    insight_type: Literal["Recommendation"] = "Recommendation"
    severity = 10


class BuildError(InsightDefinition):
    """A confirmed problem. The resource cannot be built or deployed as configured."""

    insight_type: Literal["Error"] = BUILD_ERROR_TYPE
    severity = 50


class ParseFileError(BuildError):
    title = "Failed to parse file"


class ReadFileError(BuildError):
    title = "Failed to read file"


class FileSyntaxError(BuildError):
    title = "Syntax error"


class InvalidContentError(BuildError):
    title = "Invalid content"


class BuildWarning(InsightDefinition):
    """A potential problem that could not be confirmed, or an issue that does not block the build."""

    insight_type: Literal["Warning"] = BUILD_WARNING_TYPE
    severity = 20


Insight = Annotated[
    ModelSyntaxError
    | ModelSyntaxWarning
    | ConsistencyError
    | Recommendation
    | FileReadError
    | IgnoredFileWarning
    | BuildError
    | BuildWarning,
    Field(discriminator="insight_type"),
]


def error_insight_type(
    legacy_type: type[T_Insight], error_type: type[BuildError] = BuildError
) -> type[T_Insight] | type[BuildError]:
    """The insight class to use for errors: error_type with the v09 flag, otherwise the legacy class."""
    return error_type if Flags.V09.is_enabled() else legacy_type


def warning_insight_type(legacy_type: type[T_Insight]) -> type[T_Insight] | type[BuildWarning]:
    """The insight class to use for warnings: BuildWarning with the v09 flag, otherwise the legacy class."""
    return BuildWarning if Flags.V09.is_enabled() else legacy_type


InsightListAdapter: TypeAdapter[list[Insight]] = TypeAdapter(list[Insight])


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
        return any(isinstance(insight, (ModelSyntaxError, BuildError)) for insight in self.data)

    @property
    def has_errors(self) -> bool:
        """Returns True if there are any errors (model syntax or consistency) in the insights."""
        return any(isinstance(insight, (ModelSyntaxError, ConsistencyError, BuildError)) for insight in self.data)

    @property
    def summary(self) -> dict[str, int]:
        """Returns a summary dict with breakdown of insights by type.

        Returns:
            Dict with keys: syntax_errors, consistency_errors, recommendations
        """

        return dict(Counter(insight.insight_type for insight in self.data))

    def dump(self) -> list[dict[str, Any]]:
        """Returns a list of insight dicts with keys insight_type, code, source_file, message, fix."""
        return [insight.model_dump() for insight in self.data]

    def to_csv(self) -> str:
        """Returns a CSV formatted string representation of the insights.

        Uses a Unix-style CSV dialect (LF-only record separators, all fields quoted) so
        ``message`` and ``fix`` may contain newlines without corrupting row boundaries.
        Carriage returns inside cells are normalized to LF newlines.

        Returns:
            CSV formatted string with columns: insight_type, code, source_file, message, fix
        """
        field_names = list(InsightDefinition.model_fields.keys())
        with io.StringIO() as output:
            writer = csv.DictWriter(
                output,
                fieldnames=field_names,
                dialect=csv.unix_dialect,
                lineterminator="\n",
                extrasaction="ignore",
            )
            writer.writeheader()
            for insight in self.data:
                writer.writerow(insight.model_dump())
            return output.getvalue()

    def to_json(self) -> str:
        """Returns a JSON array of insight objects with keys insight_type, code, source_file, message, fix."""
        return InsightListAdapter.dump_json(self.data, indent=2, ensure_ascii=False).decode(BUILD_FOLDER_ENCODING)

    @classmethod
    def from_file(cls, path: Path, organization_dir: Path) -> Self:
        """Load insights from a CSV or JSON file written during build."""
        if path.suffix == ".json":
            return cls.from_json(path.read_text(encoding=BUILD_FOLDER_ENCODING), organization_dir)
        elif path.suffix == ".csv":
            return cls.from_csv(path.read_text(encoding=BUILD_FOLDER_ENCODING), organization_dir)
        raise ToolkitValidationError(f"Unsupported insight file format: {path.suffix}")

    @classmethod
    def from_csv(cls, content: str, organization_dir: Path) -> Self:
        """Load insights from a CSV string produced by ``to_csv``."""
        return cls(
            InsightListAdapter.validate_python(
                list(csv.DictReader(io.StringIO(content), dialect=csv.unix_dialect)),
                context={"organization_dir": organization_dir},
            )
        )

    @classmethod
    def from_json(cls, content: str, organization_dir: Path) -> Self:
        """Load insights from a JSON string produced by ``to_json``."""
        return cls(
            InsightListAdapter.validate_python(json.loads(content), context={"organization_dir": organization_dir})
        )
