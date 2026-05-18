import importlib
import inspect
import logging
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Any

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules.base import RuleDefinition, RuleType

logger = logging.getLogger(__name__)


class RulesDiscovery(ABC):
    @abstractmethod
    def discover_rules(self) -> dict[RuleType, RuleDefinition[Any]]:
        pass


class LocalRulesDiscovery(RulesDiscovery):
    _MODULE_PATH_PREFIX = "cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.rules"

    def __init__(self) -> None:
        self._rules_dir = self._get_rules_directory()

    @staticmethod
    def create() -> "LocalRulesDiscovery":
        return LocalRulesDiscovery()

    def discover_rules(self) -> dict[RuleType, RuleDefinition[Any]]:
        rules: dict[RuleType, RuleDefinition[Any]] = {}

        try:
            rule_files = self._get_rule_modules()
            for module_name in rule_files:
                try:
                    module = importlib.import_module(f"{self._MODULE_PATH_PREFIX}.{module_name}")
                    discovered_definitions = self._extract_rule_definitions(module)

                    for rule_def in discovered_definitions:
                        try:
                            instance = rule_def()
                            rule_type = instance.type()
                            rules[rule_type] = instance
                            logger.info(f"Discovered rule definition: {rule_def.__name__} -> {rule_type}")
                        except Exception as e:
                            logger.warning(f"Failed to instantiate rule definition {rule_def.__name__}: {e}")

                except ImportError as exception:
                    logger.warning(f"Failed to import rules module '{module_name}': {exception}")

        except Exception as exception:
            logger.error(f"Error during rule discovery: {exception}")
            raise RuntimeError(f"Failed to discover rules: {exception}") from exception

        if not rules:
            logger.warning("No rule definitions were discovered")

        return rules

    def _get_rules_directory(self) -> Path:
        current_file = Path(__file__).resolve()
        registry_dir = current_file.parent
        aliasing_dir = registry_dir.parent
        rules_dir = aliasing_dir / "rules"
        return rules_dir

    def _get_rule_modules(self) -> list[str]:
        py_files = sorted(self._rules_dir.glob("*.py"))
        module_names = [file.stem for file in py_files if file.stem not in ("__init__", "base")]
        return module_names

    def _extract_rule_definitions(self, module: Any) -> list[type[RuleDefinition[Any]]]:
        rule_definitions: list[type[RuleDefinition[Any]]] = []

        for _, obj in inspect.getmembers(module):
            if (
                inspect.isclass(obj)
                and issubclass(obj, RuleDefinition)
                and obj is not RuleDefinition
                and obj.__module__ == module.__name__
            ):
                rule_definitions.append(obj)

        return rule_definitions
