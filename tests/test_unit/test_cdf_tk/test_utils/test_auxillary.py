from abc import ABC

from cognite_toolkit._cdf_tk.utils._auxillary import get_get_concrete_subclasses


class GrandParent(ABC): ...


class Parent(GrandParent, ABC): ...


class Parent2(GrandParent): ...


class Child(Parent): ...


class Child2(Parent): ...


class TestGetConcreteSubclasses:
    def test_get_concrete_subclasses(self):
        subclasses = get_get_concrete_subclasses(GrandParent)

        assert subclasses == [Parent2, Child, Child2]
