from pathlib import Path

from cognite_toolkit._cdf_tk.resource_ios._base_ios import CrudResource
from cognite_toolkit._cdf_tk.resource_ios._data_organization import CatalogDataSetsIO
from cognite_toolkit._cdf_tk.yaml_classes.catalog_dataset import CatalogDataSetYAML


def test_catalog_data_set_crud_roundtrip() -> None:
    catalog = CatalogDataSetYAML.model_validate(
        {
            "externalId": "ds_catalog",
            "name": "Catalog set",
            "description": "Owned by the catalog",
            "writeProtected": True,
            "rawTables": [{"databaseName": "db", "tableName": "tbl"}],
            "archived": False,
            "consoleGoverned": True,
            "consoleOwners": [{"name": "Alice", "email": "alice@example.com"}],
            "consoleExtractors": {"accounts": ["acc1"]},
            "transformations": [{"name": "my-transform", "type": "jetfire", "details": "some details"}],
            "consoleSource": {"names": ["source1"]},
            "consoleAdditionalDocs": [{"type": "url", "id": "https://example.com", "name": "Docs"}],
            "metadata": {"extraKey": "info"},
        }
    ).model_dump(by_alias=True)

    crud_resources = [
        resource
        for resource in CatalogDataSetsIO.to_crud_types(
            catalog, Path("dummy"), CatalogDataSetYAML.model_validate(catalog)
        )
        if isinstance(resource, CrudResource)
    ]

    assert len(crud_resources) == 1
    assert CatalogDataSetsIO.from_crud_types(crud_resources) == catalog
