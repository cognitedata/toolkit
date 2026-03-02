from cognite_toolkit._cdf_tk.commands._migrate.infield_data_mappings import create_infield_data_mappings


class TestInFieldMapping:
    def test_mapping_validation(self) -> None:
        _ = create_infield_data_mappings()
        assert True, "Mapping creation should not raise an error"
