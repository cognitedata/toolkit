from cognite.client import data_modeling as dm
from pathlib import Path

from cognite.client.data_classes.data_modeling.views import ViewPropertyApply, ConnectionDefinitionApply, \
    ReverseDirectRelationApply

DATA_MODELS = Path(__file__).resolve().parent / 'data_models'

MY_SCHEMA_SPACE = "sp_my_schema"
MY_ORG = "MyOrganization"
COGNITE = "Cognite"

def main() -> None:
    loaded_views = (dm.ViewApply.load(view_file.read_text()) for view_file in (DATA_MODELS / "views").iterdir())
    view_by_id = {view.as_id(): view for view in loaded_views}


    for view_file in (DATA_MODELS / "views").iterdir():
        view = dm.ViewApply.load(view_file.read_text())
        view.external_id = view.external_id.replace(COGNITE, MY_ORG)
        view.space = MY_SCHEMA_SPACE

        properties = dict(view.properties or {})
        check = (view.implements or []).copy()
        seen = set(check)
        while check:
            parent = check.pop()
            properties.update(view_by_id[parent].properties or {})
            for grandparent in view_by_id[parent].implements or []:
                if grandparent not in seen:
                    check.append(grandparent)
                    seen.add(grandparent)

        view.implements = []

        view.properties = {
            prop_id: change_prop(prop)
            for prop_id, prop in properties.items()
        }

        new_file = view_file.parent / view_file.name.replace(COGNITE, "")
        raw_yaml = view.dump_yaml()
        raw_yaml = raw_yaml.replace(MY_ORG, "{{ organization }}").replace(MY_SCHEMA_SPACE, "{{ schema_space }}")
        new_file.write_text(raw_yaml)
        view_file.unlink()


def change_prop(prop: ViewPropertyApply) -> ViewPropertyApply:
    if isinstance(prop, dm.MappedPropertyApply | ConnectionDefinitionApply | ReverseDirectRelationApply) and prop.source:
        prop.source = as_my_view(prop.source)

    elif isinstance(prop, ReverseDirectRelationApply):

        prop.through = dm.PropertyId(
            source=as_my_view(prop.through.source),
            property=prop.through.property
        )

    return prop

def as_my_view(source: dm.ViewId) -> dm.ViewId:
    return dm.ViewId(space=MY_SCHEMA_SPACE, external_id=source.external_id.replace(COGNITE, MY_ORG), version=source.version)


if __name__ == '__main__':
    main()
