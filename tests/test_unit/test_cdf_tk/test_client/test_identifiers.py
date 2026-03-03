from cognite.neat._utils.auxiliary import get_concrete_subclasses

from cognite_toolkit._cdf_tk.client._resource_base import Identifier

# Import one of the concrete Identifier subclasses to ensure they are registered in the test
from cognite_toolkit._cdf_tk.client.identifiers import NodeReference


class TestIdentifiers:
    def test_all_identifiers_suffix_id(self) -> None:
        subclasses = set(get_concrete_subclasses(Identifier))

        assert NodeReference in subclasses, "NodeReference should be a subclass of Identifier"

        not_suffix_id = [cls.__name__ for cls in subclasses if not cls.__name__.endswith("Id")]
        assert not_suffix_id == [], f"The following Identifier subclasses do not end with 'Id': {not_suffix_id}"
