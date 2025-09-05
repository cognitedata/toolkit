from cognite_toolkit._cdf_tk.client.data_classes.search_config import (
    SearchConfig,
    SearchConfigList,
    SearchConfigViewProperty,
    SearchConfigWrite,
    SearchConfigWriteList,
    ViewId,
)


class TestSearchConfigView:
    def test_search_config_view(self):
        # Test initialization
        view = ViewId(external_id="test-view", space="test-space")
        assert view.external_id == "test-view"
        assert view.space == "test-space"

        # Test loading from dictionary
        data = {"externalId": "test-view", "space": "test-space"}
        loaded_view = ViewId.load(data)
        assert loaded_view.external_id == "test-view"
        assert loaded_view.space == "test-space"

        # Test dumping to dictionary with camelCase
        dumped = view.dump()
        assert dumped == {"externalId": "test-view", "space": "test-space"}

        # Test dumping to dictionary with snake_case
        dumped_snake = view.dump(camel_case=False)
        assert dumped_snake == {"external_id": "test-view", "space": "test-space"}


class TestSearchConfigViewProperty:
    def test_search_config_view_property(self):
        # Test initialization
        prop = SearchConfigViewProperty(property="test-prop", disabled=True, selected=False, hidden=True)
        assert prop.property == "test-prop"
        assert prop.disabled is True
        assert prop.selected is False
        assert prop.hidden is True

        # Test loading from dictionary
        data = {"property": "test-prop", "disabled": True, "selected": False, "hidden": True}
        loaded_prop = SearchConfigViewProperty.load(data)
        assert loaded_prop.property == "test-prop"
        assert loaded_prop.disabled is True
        assert loaded_prop.selected is False
        assert loaded_prop.hidden is True

        # Test dumping to dictionary
        dumped = prop.dump()
        assert dumped == {"property": "test-prop", "disabled": True, "selected": False, "hidden": True}


class TestSearchConfigWrite:
    def test_init(self):
        view = ViewId(external_id="test-view", space="test-space")
        property_1 = SearchConfigViewProperty(property="prop1", selected=True)
        property_2 = SearchConfigViewProperty(property="prop2", disabled=True)

        config = SearchConfigWrite(
            view=view,
            id=123,
            use_as_name="name-prop",
            use_as_description="desc-prop",
            columns_layout=[property_1],
            filter_layout=[property_2],
            properties_layout=[property_1, property_2],
        )

        assert config.view == view
        assert config.id == 123
        assert config.use_as_name == "name-prop"
        assert config.use_as_description == "desc-prop"
        assert config.columns_layout == [property_1]
        assert config.filter_layout == [property_2]
        assert config.properties_layout == [property_1, property_2]

    def test_load(self):
        data = {
            "id": 123,
            "view": {"externalId": "test-view", "space": "test-space"},
            "useAsName": "name-prop",
            "useAsDescription": "desc-prop",
            "columnsLayout": [{"property": "prop1", "selected": True}],
            "filterLayout": [{"property": "prop2", "disabled": True}],
            "propertiesLayout": [{"property": "prop1", "selected": True}, {"property": "prop2", "disabled": True}],
        }

        config = SearchConfigWrite.load(data)

        assert config.id == 123
        assert config.view.external_id == "test-view"
        assert config.view.space == "test-space"
        assert config.use_as_name == "name-prop"
        assert config.use_as_description == "desc-prop"
        assert len(config.columns_layout) == 1
        assert config.columns_layout[0].property == "prop1"
        assert config.columns_layout[0].selected is True
        assert len(config.filter_layout) == 1
        assert config.filter_layout[0].property == "prop2"
        assert config.filter_layout[0].disabled is True
        assert len(config.properties_layout) == 2

    def test_dump(self):
        view = ViewId(external_id="test-view", space="test-space")
        property_1 = SearchConfigViewProperty(property="prop1", selected=True)

        config = SearchConfigWrite(
            view=view,
            id=123,
            use_as_name="name-prop",
            columns_layout=[property_1],
        )

        dumped = config.dump()

        assert dumped["id"] == 123
        assert dumped["useAsName"] == "name-prop"
        assert dumped["view"] == {"externalId": "test-view", "space": "test-space"}
        assert dumped["columnsLayout"] == [{"property": "prop1", "selected": True}]


class TestSearchConfig:
    def test_as_write(self):
        view = ViewId(external_id="test-view", space="test-space")

        config = SearchConfig(view=view, id=123, created_time=1000, updated_time=2000, use_as_name="name-prop")

        write_config = config.as_write()

        assert isinstance(write_config, SearchConfigWrite)
        assert write_config.id == config.id
        assert write_config.view == config.view
        assert write_config.use_as_name == config.use_as_name

    def test_load(self):
        data = {
            "id": 123,
            "createdTime": 1000,
            "lastUpdatedTime": 2000,
            "view": {"externalId": "test-view", "space": "test-space"},
            "useAsName": "name-prop",
        }

        config = SearchConfig.load(data)

        assert config.id == 123
        assert config.created_time == 1000
        assert config.updated_time == 2000
        assert config.view.external_id == "test-view"
        assert config.use_as_name == "name-prop"


class TestSearchConfigList:
    def test_as_write(self):
        view1 = ViewId(external_id="view1", space="space1")
        view2 = ViewId(external_id="view2", space="space2")

        config1 = SearchConfig(view=view1, id=1, created_time=1000, updated_time=2000)
        config2 = SearchConfig(view=view2, id=2, created_time=3000, updated_time=4000)

        config_list = SearchConfigList([config1, config2])
        write_list = config_list.as_write()

        assert isinstance(write_list, SearchConfigWriteList)
        assert len(write_list) == 2
        assert all(isinstance(item, SearchConfigWrite) for item in write_list)
        assert write_list[0].id == 1
        assert write_list[1].id == 2
