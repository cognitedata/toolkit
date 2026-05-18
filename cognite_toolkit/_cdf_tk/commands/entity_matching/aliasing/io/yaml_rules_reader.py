from typing import Any, ClassVar

import yaml

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import AliasingRule
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.io.errors import InvalidRuleFormatError, YamlReadError


class YamlRulesReader:
    REQUIRED_FIELDS: ClassVar[set[str]] = {"name", "rule_type", "description", "payload"}

    def read_file(self, file_path: str) -> list[AliasingRule]:
        raw_data = self._load_yaml_file(file_path)
        self._validate_rules_key_exists(raw_data, file_path)

        rules_data = raw_data.get("rules")
        self._validate_rules_is_list(rules_data, file_path)

        rules: list[AliasingRule] = []
        for index, rule_data in enumerate(rules_data):
            rule = self._validate_and_construct_rule(rule_data, index)
            rules.append(rule)

        return rules

    def _load_yaml_file(self, file_path: str) -> Any:
        try:
            with open(file_path, encoding="utf-8") as f:
                return yaml.safe_load(f)
        except FileNotFoundError as e:
            raise YamlReadError(
                "File not found",
                file_path=file_path,
            ) from e
        except yaml.YAMLError as e:
            raise YamlReadError(
                f"Invalid YAML syntax: {e!s}",
                file_path=file_path,
            ) from e
        except Exception as e:
            raise YamlReadError(
                f"Error reading file: {e!s}",
                file_path=file_path,
            ) from e

    def _validate_rules_key_exists(self, raw_data: Any, file_path: str) -> None:
        if raw_data is None:
            raise YamlReadError(
                "YAML file is empty or contains only comments",
                file_path=file_path,
            )

        if not isinstance(raw_data, dict):
            raise YamlReadError(
                f"Root of YAML must be a mapping (dictionary), found: {type(raw_data).__name__}",
                file_path=file_path,
            )

        if "rules" not in raw_data:
            raise YamlReadError(
                f"Missing required 'rules' key. Found keys: {list(raw_data.keys())}",
                file_path=file_path,
            )

    def _validate_rules_is_list(self, rules_data: Any, file_path: str) -> None:
        if not isinstance(rules_data, list):
            raise YamlReadError(
                f"'rules' must be a list, found: {type(rules_data).__name__}",
                file_path=file_path,
            )

        if not rules_data:
            raise YamlReadError(
                "'rules' list is empty; at least one rule must be defined",
                file_path=file_path,
            )

    def _validate_and_construct_rule(
        self,
        rule_data: Any,
        index: int,
    ) -> AliasingRule:
        rule_name = None

        self._validate_rule_is_dict(rule_data, index)

        rule_name = self._extract_and_validate_field(
            rule_data,
            index,
            rule_name,
            "name",
            str,
        )

        rule_type = self._extract_and_validate_field(
            rule_data,
            index,
            rule_name,
            "rule_type",
            str,
        )

        description = self._extract_and_validate_field(
            rule_data,
            index,
            rule_name,
            "description",
            str,
        )

        payload = self._extract_and_validate_field(
            rule_data,
            index,
            rule_name,
            "payload",
            dict,
        )

        self._validate_all_fields_present(rule_data, index, rule_name)

        return AliasingRule(
            name=rule_name,
            rule_type=rule_type,
            description=description,
            payload=payload,
        )

    def _validate_rule_is_dict(self, rule_data: Any, index: int) -> None:
        if not isinstance(rule_data, dict):
            raise InvalidRuleFormatError(
                "Rule must be a mapping (dictionary)",
                rule_index=index,
                expected="dictionary",
                actual=type(rule_data).__name__,
            )

    def _extract_and_validate_field(
        self,
        rule_data: dict[str, Any],
        index: int,
        rule_name: str | None,
        field_name: str,
        expected_type: type,
    ) -> Any:
        if field_name not in rule_data:
            found_fields = list(rule_data.keys())
            raise InvalidRuleFormatError(
                f"Missing required field '{field_name}'",
                rule_index=index,
                rule_name=rule_name,
                field_name=field_name,
                expected=f"field '{field_name}' of type {expected_type.__name__}",
                actual=f"found fields: {found_fields}",
            )

        value = rule_data[field_name]

        if not isinstance(value, expected_type):
            raise InvalidRuleFormatError(
                f"Field '{field_name}' has invalid type",
                rule_index=index,
                rule_name=rule_name,
                field_name=field_name,
                expected=expected_type.__name__,
                actual=type(value).__name__,
            )

        if expected_type is str and isinstance(value, str) and not value.strip():
            raise InvalidRuleFormatError(
                f"Field '{field_name}' cannot be empty or whitespace-only",
                rule_index=index,
                rule_name=rule_name,
                field_name=field_name,
                expected="non-empty string",
                actual="empty string",
            )

        return value

    def _validate_all_fields_present(
        self,
        rule_data: dict[str, Any],
        index: int,
        rule_name: str,
    ) -> None:
        found_fields = set(rule_data.keys())
        unexpected_fields = found_fields - self.REQUIRED_FIELDS

        if unexpected_fields:
            raise InvalidRuleFormatError(
                "Rule contains unexpected fields",
                rule_index=index,
                rule_name=rule_name,
                expected=f"fields: {sorted(self.REQUIRED_FIELDS)}",
                actual=f"found extra fields: {sorted(unexpected_fields)}",
            )
