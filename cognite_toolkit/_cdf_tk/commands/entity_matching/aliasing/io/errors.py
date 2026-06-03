from cognite_toolkit._cdf_tk.exceptions import ToolkitValueError


class YamlReadError(ToolkitValueError):
    def __init__(
        self,
        message: str,
        file_path: str | None = None,
        line_number: int | None = None,
    ) -> None:
        self.file_path = file_path
        self.line_number = line_number

        error_parts = [message]
        if file_path:
            error_parts.append(f"File: {file_path}")
        if line_number:
            error_parts.append(f"Line: {line_number}")

        full_message = " | ".join(error_parts)
        super().__init__(full_message)


class InvalidRuleFormatError(ToolkitValueError):
    def __init__(
        self,
        message: str,
        rule_index: int | None = None,
        rule_name: str | None = None,
        field_name: str | None = None,
        expected: str | None = None,
        actual: str | None = None,
    ) -> None:
        self.rule_index = rule_index
        self.rule_name = rule_name
        self.field_name = field_name
        self.expected = expected
        self.actual = actual

        error_parts = [message]

        if rule_index is not None:
            error_parts.append(f"Rule index: {rule_index}")

        if rule_name:
            error_parts.append(f"Rule name: '{rule_name}'")

        if field_name:
            error_parts.append(f"Field: '{field_name}'")

        if expected:
            error_parts.append(f"Expected: {expected}")

        if actual:
            error_parts.append(f"Actual: {actual}")

        full_message = " | ".join(error_parts)
        super().__init__(full_message)
