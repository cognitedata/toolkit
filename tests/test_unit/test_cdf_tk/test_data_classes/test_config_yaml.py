from __future__ import annotations

from typing import Any

import pytest
import yaml

from cognite_toolkit._cdf_tk.data_classes import (
    ConfigEntry,
    Environment,
    InitConfigYAML,
)
from cognite_toolkit._cdf_tk.utils import YAMLComment, flatten_dict
from cognite_toolkit._cdf_tk.utils.file import yaml_safe_dump
from tests.data import PROJECT_FOR_TEST


class TestConfigYAML:
    def test_producing_correct_keys(
        self, project_for_test_config_dev_yaml: str, dummy_environment: Environment
    ) -> None:
        expected_keys = set(flatten_dict(yaml.safe_load(project_for_test_config_dev_yaml)))
        # Custom keys are not loaded from the module folder.
        # This custom key is added to the dev.config.yaml for other tests.
        expected_keys.remove(("variables", "custom_modules", "my_example_module", "transformation_is_paused"))
        # Skip all environment variables
        expected_keys = {k for k in expected_keys if not k[0] == "environment"}

        config = InitConfigYAML(dummy_environment).load_defaults(PROJECT_FOR_TEST)

        actual_keys = set(config.keys())
        missing = expected_keys - actual_keys
        assert not missing, f"Missing keys: {missing}"
        extra = actual_keys - expected_keys
        assert not extra, f"Extra keys: {extra}"

    def test_extract_extract_config_yaml_comments(self, project_for_test_config_dev_yaml: str) -> None:
        expected_comments = {
            ("variables", "modules", "a_module", "readonly_source_id"): YAMLComment(
                above=["This is a comment in the middle of the file"], after=[]
            ),
            ("variables", "modules", "another_module", "source_asset"): YAMLComment(
                above=["This is a comment at the beginning of the module."],
                after=["This is an extra comment added to the config only 'lore ipsum'"],
            ),
            ("variables", "modules", "another_module", "source_files"): YAMLComment(
                after=["This is a comment after a variable"]
            ),
        }

        actual_comments = InitConfigYAML._extract_comments(project_for_test_config_dev_yaml)

        assert actual_comments == expected_comments

    @pytest.mark.parametrize(
        "raw_file, key_prefix, expected_comments",
        [
            pytest.param(
                """---
# This is a module comment
variable: value # After variable comment
# Before variable comment
variable2: value2
variable3: 'value with #in it'
variable4: "value with #in it" # But a comment after
""",
                tuple("super_module.module_a".split(".")),
                {
                    ("super_module", "module_a", "variable"): YAMLComment(
                        after=["After variable comment"], above=["This is a module comment"]
                    ),
                    ("super_module", "module_a", "variable2"): YAMLComment(above=["Before variable comment"]),
                    ("super_module", "module_a", "variable4"): YAMLComment(after=["But a comment after"]),
                },
                id="module comments",
            )
        ],
    )
    def test_extract_default_config_comments(
        self, raw_file: str, key_prefix: tuple[str, ...], expected_comments: dict[str, Any]
    ):
        actual_comments = InitConfigYAML._extract_comments(raw_file, key_prefix)
        assert actual_comments == expected_comments

    def test_persist_variable_with_comment(self, project_for_test_config_dev_yaml: str) -> None:
        custom_comment = "This is an extra comment added to the config only 'lore ipsum'"

        config = InitConfigYAML.load_existing(project_for_test_config_dev_yaml).load_defaults(PROJECT_FOR_TEST)

        dumped = config.dump_yaml_with_comments()
        loaded = yaml.safe_load(dumped)
        assert loaded["variables"]["modules"]["another_module"]["source_asset"] == "my_new_workmate"
        assert custom_comment in dumped

    def test_added_and_removed_variables(self, project_for_test_config_dev_yaml: str) -> None:
        existing_config_yaml = yaml.safe_load(project_for_test_config_dev_yaml)
        # Added = Exists in the BUILD_CONFIG directory default.config.yaml files but not in config.yaml
        existing_config_yaml["variables"]["modules"]["another_module"].pop("source_asset")
        # Removed = Exists in config.yaml but not in the BUILD_CONFIG directory default.config.yaml files
        existing_config_yaml["variables"]["modules"]["another_module"]["removed_variable"] = "old_value"

        config = InitConfigYAML.load_existing(yaml_safe_dump(existing_config_yaml)).load_defaults(PROJECT_FOR_TEST)

        removed = [v for v in config.values() if v.default_value is None]
        # There is already a custom variable in the config.yaml file
        assert len(removed) == 2
        assert ("variables", "modules", "another_module", "removed_variable") in [v.key_path for v in removed]

        added = [v for v in config.values() if v.current_value is None]
        assert len(added) == 1
        assert added[0].key_path == ("variables", "modules", "another_module", "source_asset")

    def test_load_variables(self, dummy_environment: Environment) -> None:
        expected = {
            ("variables", "modules", "a_module", "readonly_source_id"),
            # default_location is used in two modules and is moved to the top level
            ("variables", "modules", "default_location"),
            ("variables", "modules", "another_module", "source_files"),
            ("variables", "modules", "another_module", "model_space"),
            ("variables", "modules", "parent_module", "child_module", "source_asset"),
        }

        config = InitConfigYAML(dummy_environment).load_variables(PROJECT_FOR_TEST, propagate_reused_variables=True)

        missing = expected - set(config.keys())
        extra = set(config.keys()) - expected
        assert not missing, f"Missing keys: {missing}. Got extra {extra}"
        assert not extra, f"Extra keys: {extra}"

    def test_load_parent_variables(self, dummy_environment: Environment) -> None:
        config = InitConfigYAML(
            dummy_environment,
            {
                ("variables", "modules", "infield", "shared_variable"): ConfigEntry(
                    key_path=("variables", "modules", "infield", "shared_variable"),
                    default_value="shared_value",
                )
            },
        )

        config._load_variables({"shared_variable": {("cognite_modules", "infield", "cdf_infield_common")}})

        assert ("variables", "modules", "infield", "shared_variable") in config.keys()
        assert ("variables", "modules", "infield", "cdf_infield_common", "shared_variable") not in config.keys()

    def test_trailing_slash_in_selected(self, project_for_test_config_dev_yaml: str) -> None:
        existing_config = InitConfigYAML.load_existing(project_for_test_config_dev_yaml).load_defaults(PROJECT_FOR_TEST)
        dumped_config = existing_config.dump_yaml_with_comments()

        dumped_config_dict = yaml.safe_load(dumped_config)

        assert dumped_config_dict["environment"]["selected"][0] == "modules/"

    def test_ignoring_patterns(self, dummy_environment: Environment) -> None:
        ignore_pattern = [
            ("modules", "parent_module", "*", "child_variable"),
            ("modules", "a_module", "readwrite_source_id"),
        ]

        config = InitConfigYAML(dummy_environment).load_defaults(PROJECT_FOR_TEST)
        config_with_ignore = InitConfigYAML(dummy_environment).load_defaults(
            PROJECT_FOR_TEST, ignore_patterns=ignore_pattern
        )

        ignored = set(config.keys()) - set(config_with_ignore.keys())
        assert ignored == {
            ("variables", "modules", "parent_module", "child_module", "child_variable"),
            ("variables", "modules", "a_module", "readwrite_source_id"),
        }

    def test_lift_variables(self, dummy_environment) -> None:
        config = InitConfigYAML(dummy_environment).load_defaults(PROJECT_FOR_TEST)

        config.lift()

        assert ("variables", "modules", "dataset") in config.keys()
        assert ("variables", "modules", "a_module", "dataset") not in config.keys()
        assert ("variables", "modules", "another_module", "dataset") not in config.keys()
        assert ("variables", "modules", "another_module", "source_asset") in config.keys()
        assert ("variables", "modules", "parent_module", "child_module", "source_asset") in config.keys()

    def test_allow_wide_lines(self, dummy_environment) -> None:
        long_default_value = "value " * 20  # whitespace makes default yaml.dump split lines

        config = InitConfigYAML(
            dummy_environment,
            {
                ("variables", "modules", "infield", "shared_variable"): ConfigEntry(
                    key_path=("variables", "modules", "infield", "shared_variable"),
                    default_value=long_default_value,
                )
            },
        )

        dumped = config.dump_yaml_with_comments()
        loaded = yaml.safe_load(dumped)
        assert loaded["variables"]["modules"]["infield"]["shared_variable"] == long_default_value
