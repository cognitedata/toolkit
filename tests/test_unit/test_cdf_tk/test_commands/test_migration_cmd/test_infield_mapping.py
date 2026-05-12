from unittest.mock import MagicMock

from cognite_toolkit._cdf_tk.commands._migrate.conversion import InFieldConditionMapping
from cognite_toolkit._cdf_tk.commands._migrate.infield_data_mappings import (
    create_infield_data_mappings,
    create_infield_schedule_selector,
)


class TestInFieldMapping:
    def test_mapping_validation(self) -> None:
        mappings = create_infield_data_mappings(extra="forbid")
        assert mappings, "Mapping creation should not raise an error"

    def test_schedule_selector_validation(self) -> None:
        selector = create_infield_schedule_selector()
        assert selector, "Schedule selector creation should not raise an error"

    def test_condition_source_view_translates_legacy_version(self) -> None:
        condition_mapping = InFieldConditionMapping(create_infield_data_mappings())
        context = MagicMock()

        result = condition_mapping.convert({"sourceView": "cdf_apm/TemplateItem/v5"}, context)

        assert result.errors == []
        assert result.container_properties == {"sourceView": "cdf_infield/TemplateItem/v1"}

    def test_condition_source_view_unknown_logical_view_errors(self) -> None:
        condition_mapping = InFieldConditionMapping(create_infield_data_mappings())
        context = MagicMock()

        result = condition_mapping.convert({"sourceView": "cdf_apm/Bogus/v1"}, context)

        assert result.container_properties == {}
        assert len(result.errors) == 1
        assert "Unexpected sourceView value" in result.errors[0]
