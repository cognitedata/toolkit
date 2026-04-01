from cognite.neat._utils.auxiliary import get_concrete_subclasses

from cognite_toolkit._cdf_tk.client._resource_base import Identifier

# Import one of the concrete Identifier subclasses to ensure they are registered in the test
from cognite_toolkit._cdf_tk.client.identifiers import EdgeTypeId, NodeId, ViewId, ViewNoVersionId


class TestIdentifiers:
    def test_all_identifiers_suffix_id(self) -> None:
        """Test that all identifier classes are subclasses of Identifier and that their names end with 'Id'.

        This is to make sure that it is clear that these are identifiers and avoid confusing them with other classes.
        """
        subclasses = set(get_concrete_subclasses(Identifier))

        assert NodeId in subclasses, "NodeReference should be a subclass of Identifier"

        not_suffix_id = [cls.__name__ for cls in subclasses if not cls.__name__.endswith("Id")]
        assert not_suffix_id == [], f"The following Identifier subclasses do not end with 'Id': {not_suffix_id}"


class TestEdgeTypeId:
    def test_validate_str(self) -> None:
        edge_type = EdgeTypeId(type=NodeId(space="test_space", external_id="test_external_id"), direction="outwards")

        assert edge_type == EdgeTypeId.model_validate(str(edge_type))


class TestViewIdEquality:
    def test_view_no_version_equals_versioned_view(self) -> None:
        v1 = ViewId(space="test_space", external_id="test_external_id", version="v1")
        v2 = ViewId(space="test_space", external_id="test_external_id", version="v2")
        no_version = ViewNoVersionId(space=v1.space, external_id=v1.external_id)

        assert v1 != v2
        assert no_version == v1
        assert v2 == no_version
