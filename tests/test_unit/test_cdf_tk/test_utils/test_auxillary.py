from abc import ABC, abstractmethod

from cognite_toolkit._cdf_tk.utils._auxillary import get_concrete_subclasses


class GrandParent(ABC):
    @abstractmethod
    def method(self) -> None:
        pass


class Parent(GrandParent, ABC): ...


class Parent2(GrandParent):
    def method(self) -> None:
        print("Parent2 method implementation")


class Child(Parent):
    def method(self) -> None:
        print("Child method implementation")


class Child2(Parent):
    def method(self) -> None:
        print("Child2 method implementation")


class Child3(Parent2):
    def method(self) -> None:
        print("Child3 method implementation")


class TestGetConcreteSubclasses:
    def test_get_concrete_subclasses(self):
        subclasses = get_concrete_subclasses(GrandParent)

        assert set(subclasses) == {Parent2, Child, Child2, Child3}
