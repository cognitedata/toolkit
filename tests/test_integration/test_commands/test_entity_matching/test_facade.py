import json

import pytest
from kuiper import compile_expression

from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.assembly.aliasing_kuiper_builder import AliasingRule
from cognite_toolkit._cdf_tk.commands.entity_matching.aliasing.bootstrap.bootstrapper import provide_aliasing_facade


def execute_kuiper_transformation(expression: str, kuiper_input_json: str) -> str:
    compiled = compile_expression(expression, ["input"])
    try:
        # Kuiper >= 0.19.0 (Python >= 3.12) - returns JSON string directly
        return compiled.run_json(kuiper_input_json)
    except AttributeError:
        # Kuiper < 0.19.0 (Python < 3.12) - may return dict or JSON string
        result = compiled.run(kuiper_input_json)
        if isinstance(result, str):
            return result
        return json.dumps(result)


class TestAliasingFacadeIntegration:
    def test_when_generate_with_single_character_substitution_then_expression_compiles_and_transforms_correctly(
        self,
    ) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="p_to_k",
            rule_type="character_substitution",
            description="Replace P with K",
            payload={"replacements": {"P": "K"}},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P_101"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["P_101"],
                "aliases": ["K_101"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_multiple_character_substitutions_then_all_transformations_applied(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="multi_char_sub",
            rule_type="character_substitution",
            description="Replace P with K and C with D",
            payload={"replacements": {"P": "K", "C": "D"}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P_101"]},
                {"space": "test_space", "external_id": "asset_002", "keys": ["C_000"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["P_101"],
                "aliases": ["K_101"],
            },
            {
                "space": "test_space",
                "external_id": "asset_002",
                "keys": ["C_000"],
                "aliases": ["D_000"],
            },
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_character_substitution_sample_then_transforms_as_expected(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="character_substitution",
            rule_type="character_substitution",
            description="Character substitution for aliasing",
            payload={"replacements": {"C": "D", "P": "K"}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P_101"]},
                {"space": "test_space", "external_id": "asset_002", "keys": ["C_000"]},
            ]
        }

        expected_output = [
            {
                "aliases": ["K_101"],
                "external_id": "asset_001",
                "keys": ["P_101"],
                "space": "test_space",
            },
            {
                "aliases": ["D_000"],
                "external_id": "asset_002",
                "keys": ["C_000"],
                "space": "test_space",
            },
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_multiple_keys_then_all_keys_get_aliases(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="transform",
            rule_type="character_substitution",
            description="Replace P with K and C with D",
            payload={"replacements": {"P": "K", "C": "D"}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "test_space",
                    "external_id": "asset_001",
                    "keys": ["P_101", "P_202", "C_000"],
                },
            ]
        }

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["P_101", "P_202", "C_000"],
                "aliases": ["K_101", "K_202", "D_000"],
            }
        ]

        assert output_data == expected_output

    def test_when_generate_preserves_entity_metadata_while_transforming_keys(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="test_rule",
            rule_type="character_substitution",
            description="Test rule",
            payload={"replacements": {"X": "Y"}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "my_custom_space",
                    "external_id": "my_asset_123",
                    "keys": ["X_test"],
                },
            ]
        }

        expected_output = [
            {
                "space": "my_custom_space",
                "external_id": "my_asset_123",
                "keys": ["X_test"],
                "aliases": ["Y_test"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_empty_keys_array_then_produces_empty_aliases(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="test_rule",
            rule_type="character_substitution",
            description="Test rule",
            payload={"replacements": {"A": "B"}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "test_space",
                    "external_id": "asset_001",
                    "keys": [],
                },
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": [],
                "aliases": [],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_expression_is_valid_kuiper_syntax(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="syntax_test",
            rule_type="character_substitution",
            description="Test valid syntax",
            payload={"replacements": {"A": "B"}},
        )

        kuiper_result = facade.generate([rule])

        try:
            compile_expression(kuiper_result.expression, ["input"])
        except Exception as e:
            pytest.fail(f"Generated expression is not valid Kuiper syntax: {e}")

    def test_when_generate_with_special_characters_in_substitution_then_transforms_correctly(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="special_char_sub",
            rule_type="character_substitution",
            description="Replace ö with o",
            payload={"replacements": {"ö": "o"}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "test_space",
                    "external_id": "asset_001",
                    "keys": ["körper_001"],
                },
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["körper_001"],
                "aliases": ["korper_001"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_chained_substitutions_then_applies_in_correct_order(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="chained_sub",
            rule_type="character_substitution",
            description="Replace A with B then B with C",
            payload={"replacements": {"A": "B", "B": "C"}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "test_space",
                    "external_id": "asset_001",
                    "keys": ["A_001"],
                },
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["A_001"],
                "aliases": ["C_001"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_non_matching_characters_then_keys_unchanged(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="partial_match",
            rule_type="character_substitution",
            description="Replace Z with Y",
            payload={"replacements": {"Z": "Y"}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "test_space",
                    "external_id": "asset_001",
                    "keys": ["A_001", "B_002"],
                },
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["A_001", "B_002"],
                "aliases": ["A_001", "B_002"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_simple_regex_substitution_then_expression_compiles_and_transforms_correctly(
        self,
    ) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="regex_prefix",
            rule_type="regex_substitution",
            description="Replace ASSET_ prefix with ALT_ASSET_",
            payload={"pattern": "^ASSET_", "replacement": "ALT_ASSET_"},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["ASSET_001", "ASSET_002"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["ASSET_001", "ASSET_002"],
                "aliases": ["ALT_ASSET_001", "ALT_ASSET_002"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_regex_substitution_pattern_with_no_matches_then_keys_unchanged(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="no_match_regex",
            rule_type="regex_substitution",
            description="Replace Z with Y",
            payload={"pattern": "^Z_", "replacement": "Y_"},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["ASSET_001", "ASSET_002"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["ASSET_001", "ASSET_002"],
                "aliases": ["ASSET_001", "ASSET_002"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_regex_substitution_multiple_keys_then_all_transformed(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="regex_multi",
            rule_type="regex_substitution",
            description="Replace numeric suffix pattern",
            payload={"pattern": "_000$", "replacement": "_999"},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["KEY_000", "KEY_001", "NAME_000"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["KEY_000", "KEY_001", "NAME_000"],
                "aliases": ["KEY_999", "KEY_001", "NAME_999"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_regex_substitution_empty_keys_then_empty_aliases(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="regex_empty",
            rule_type="regex_substitution",
            description="Test rule",
            payload={"pattern": "^TEST_", "replacement": "PROD_"},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "test_space",
                    "external_id": "asset_001",
                    "keys": [],
                },
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": [],
                "aliases": [],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_regex_substitution_preserves_entity_metadata(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="regex_metadata",
            rule_type="regex_substitution",
            description="Replace pattern",
            payload={"pattern": "OLD", "replacement": "NEW"},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "my_custom_space",
                    "external_id": "my_asset_123",
                    "keys": ["OLD_key_OLD"],
                },
            ]
        }

        expected_output = [
            {
                "space": "my_custom_space",
                "external_id": "my_asset_123",
                "keys": ["OLD_key_OLD"],
                "aliases": ["NEW_key_OLD"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_regex_substitution_expression_is_valid_kuiper_syntax(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="regex_syntax",
            rule_type="regex_substitution",
            description="Test valid syntax",
            payload={"pattern": "^TEST", "replacement": "PROD"},
        )

        kuiper_result = facade.generate([rule])

        try:
            compile_expression(kuiper_result.expression, ["input"])
        except Exception as e:
            pytest.fail(f"Generated expression is not valid Kuiper syntax: {e}")

    def test_when_generate_with_prefix_only_then_expression_compiles_and_transforms_correctly(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="add_prefix",
            rule_type="prefix_suffix",
            description="Add PRE_ prefix",
            payload={"prefix": "PRE_"},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["key_001"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["key_001"],
                "aliases": ["PRE_key_001"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_suffix_only_then_expression_compiles_and_transforms_correctly(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="add_suffix",
            rule_type="prefix_suffix",
            description="Add _SUF suffix",
            payload={"suffix": "_SUF"},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["key_001"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["key_001"],
                "aliases": ["key_001_SUF"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_prefix_and_suffix_then_expression_compiles_and_transforms_correctly(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="add_prefix_suffix",
            rule_type="prefix_suffix",
            description="Add PRE_ prefix and _SUF suffix",
            payload={"prefix": "PRE_", "suffix": "_SUF"},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["key_001", "key_002"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["key_001", "key_002"],
                "aliases": ["PRE_key_001_SUF", "PRE_key_002_SUF"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_prefix_suffix_empty_keys_then_empty_aliases(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="prefix_suffix_empty",
            rule_type="prefix_suffix",
            description="Add prefix and suffix",
            payload={"prefix": "PRE_", "suffix": "_SUF"},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "test_space",
                    "external_id": "asset_001",
                    "keys": [],
                },
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": [],
                "aliases": [],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_prefix_suffix_expression_is_valid_kuiper_syntax(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="prefix_suffix_syntax",
            rule_type="prefix_suffix",
            description="Test valid syntax",
            payload={"prefix": "TEST_", "suffix": "_PROD"},
        )

        kuiper_result = facade.generate([rule])

        try:
            compile_expression(kuiper_result.expression, ["input"])
        except Exception as e:
            pytest.fail(f"Generated expression is not valid Kuiper syntax: {e}")

    def test_when_generate_with_case_transformation_uppercase_then_expression_compiles_and_transforms_correctly(
        self,
    ) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="uppercase_transform",
            rule_type="case_transformation",
            description="Transform to uppercase",
            payload={"strategy": "UPPERCASE"},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["key_lower", "MiXeD_CaSe"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["key_lower", "MiXeD_CaSe"],
                "aliases": ["KEY_LOWER", "MIXED_CASE"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_case_transformation_lowercase_then_expression_compiles_and_transforms_correctly(
        self,
    ) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="lowercase_transform",
            rule_type="case_transformation",
            description="Transform to lowercase",
            payload={"strategy": "LOWERCASE"},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_002", "keys": ["KEY_UPPER", "MiXeD_CaSe"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_002",
                "keys": ["KEY_UPPER", "MiXeD_CaSe"],
                "aliases": ["key_upper", "mixed_case"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_case_transformation_expression_is_valid_kuiper_syntax(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="case_syntax",
            rule_type="case_transformation",
            description="Test valid syntax",
            payload={"strategy": "UPPERCASE"},
        )

        kuiper_result = facade.generate([rule])

        try:
            compile_expression(kuiper_result.expression, ["input"])
        except Exception as e:
            pytest.fail(f"Generated expression is not valid Kuiper syntax: {e}")

    def test_when_generate_with_single_value_expansion_then_expression_compiles_and_transforms_correctly(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="expand_p",
            rule_type="value_expansion",
            description="Expand P to PUMP and PMP",
            payload={"expansions": {"P": ["PUMP", "PMP"]}},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P-101"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["P-101"],
                "aliases": ["PUMP-101", "PMP-101"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_multiple_abbreviations_then_all_combinations_generated(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="expand_multiple",
            rule_type="value_expansion",
            description="Expand P and M abbreviations",
            payload={"expansions": {"P": ["PUMP", "PMP"], "M": ["MOTOR", "MOT"]}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P-101-M"]},
            ]
        }

        expected_aliases = {"PUMP-101-MOTOR", "PUMP-101-MOT", "PMP-101-MOTOR", "PMP-101-MOT"}

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data[0]["space"] == "test_space"
        assert output_data[0]["external_id"] == "asset_001"
        assert output_data[0]["keys"] == ["P-101-M"]
        assert set(output_data[0]["aliases"]) == expected_aliases

    def test_when_generate_with_no_matching_abbreviations_then_key_passes_through(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="expand_no_match",
            rule_type="value_expansion",
            description="Expand P abbreviation",
            payload={"expansions": {"P": ["PUMP"], "M": ["MOTOR"]}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["X-101"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["X-101"],
                "aliases": ["X-101"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_partial_abbreviation_match_then_only_matched_expanded(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="expand_partial",
            rule_type="value_expansion",
            description="Expand P and M abbreviations",
            payload={"expansions": {"P": ["PUMP", "PMP"], "M": ["MOTOR"]}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P-101"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["P-101"],
                "aliases": ["PUMP-101", "PMP-101"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_value_expansion_empty_keys_then_produces_empty_aliases(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="expand_empty",
            rule_type="value_expansion",
            description="Expand P abbreviation",
            payload={"expansions": {"P": ["PUMP"]}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "test_space",
                    "external_id": "asset_001",
                    "keys": [],
                },
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": [],
                "aliases": [],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_value_expansion_expression_is_valid_kuiper_syntax(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="expand_syntax",
            rule_type="value_expansion",
            description="Test valid syntax",
            payload={"expansions": {"P": ["PUMP", "PMP"]}},
        )

        kuiper_result = facade.generate([rule])

        try:
            compile_expression(kuiper_result.expression, ["input"])
        except Exception as e:
            pytest.fail(f"Generated expression is not valid Kuiper syntax: {e}")

    def test_when_generate_with_value_expansion_multiple_keys_then_all_transformed(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="expand_multi_keys",
            rule_type="value_expansion",
            description="Expand P abbreviation",
            payload={"expansions": {"P": ["PUMP", "PMP"]}},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P-101", "P-202", "X-303"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["P-101", "P-202", "X-303"],
                "aliases": ["PUMP-101", "PMP-101", "PUMP-202", "PMP-202", "X-303"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_leading_zero_normalization_padding_then_expression_compiles_and_transforms_correctly(
        self,
    ) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="normalize_zeros_pad",
            rule_type="leading_zero_normalization",
            description="Normalize to 5-digit numeric values",
            payload={"target_length": 5},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P-101"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["P-101"],
                "aliases": ["P-00101"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_leading_zero_normalization_stripping_then_excess_zeros_removed(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="normalize_zeros_strip",
            rule_type="leading_zero_normalization",
            description="Normalize to 3-digit numeric values",
            payload={"target_length": 3},
        )

        kuiper_result = facade.generate([rule])
        assert kuiper_result.expression is not None

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P-00101"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["P-00101"],
                "aliases": ["P-101"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_leading_zero_normalization_multiple_keys_then_all_transformed(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="normalize_multi_keys",
            rule_type="leading_zero_normalization",
            description="Normalize to 4-digit values",
            payload={"target_length": 4},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {"space": "test_space", "external_id": "asset_001", "keys": ["P-1", "P-12", "P-123", "P-1234"]},
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": ["P-1", "P-12", "P-123", "P-1234"],
                "aliases": ["P-0001", "P-0012", "P-0123", "P-1234"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_with_leading_zero_normalization_empty_keys_then_produces_empty_aliases(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="normalize_empty",
            rule_type="leading_zero_normalization",
            description="Normalize to 5-digit values",
            payload={"target_length": 5},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "test_space",
                    "external_id": "asset_001",
                    "keys": [],
                },
            ]
        }

        expected_output = [
            {
                "space": "test_space",
                "external_id": "asset_001",
                "keys": [],
                "aliases": [],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output

    def test_when_generate_leading_zero_normalization_expression_is_valid_kuiper_syntax(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="normalize_syntax",
            rule_type="leading_zero_normalization",
            description="Test valid syntax",
            payload={"target_length": 5},
        )

        kuiper_result = facade.generate([rule])

        try:
            compile_expression(kuiper_result.expression, ["input"])
        except Exception as e:
            pytest.fail(f"Generated expression is not valid Kuiper syntax: {e}")

    def test_when_generate_with_leading_zero_normalization_preserves_entity_metadata(self) -> None:
        facade = provide_aliasing_facade()

        rule = AliasingRule(
            name="normalize_metadata",
            rule_type="leading_zero_normalization",
            description="Normalize with metadata preservation",
            payload={"target_length": 6},
        )

        kuiper_result = facade.generate([rule])

        input_data = {
            "keys": [
                {
                    "space": "custom_space",
                    "external_id": "custom_asset_123",
                    "keys": ["ID-42"],
                },
            ]
        }

        expected_output = [
            {
                "space": "custom_space",
                "external_id": "custom_asset_123",
                "keys": ["ID-42"],
                "aliases": ["ID-000042"],
            }
        ]

        input_json = json.dumps(input_data)
        output_json = execute_kuiper_transformation(kuiper_result.expression, input_json)
        output_data = json.loads(output_json)

        assert output_data == expected_output
