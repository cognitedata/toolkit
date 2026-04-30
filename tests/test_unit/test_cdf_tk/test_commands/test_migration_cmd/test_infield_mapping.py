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

    def test_skip_observations_filters_mapping(self) -> None:
        all_mappings = create_infield_data_mappings()
        filtered = [m for m in all_mappings if m.destination_view.external_id != "FieldObservation"]
        assert len(filtered) == len(all_mappings) - 1
        assert all(m.destination_view.external_id != "FieldObservation" for m in filtered)
