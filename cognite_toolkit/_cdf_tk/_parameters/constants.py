BASE_TYPES = {t.__name__ for t in (str, int, float, bool)}
CONTAINER_TYPES = {t.__name__ for t in (list, dict)}
TYPES = BASE_TYPES | CONTAINER_TYPES


class _AnyInt(int):
    def __eq__(self, other: object) -> bool:
        if isinstance(other, int):
            return True
        return NotImplemented

    def __hash__(self) -> int:
        return id(ANY_INT)

    def __str__(self) -> str:
        return "AnyInt"

    def __repr__(self) -> str:
        return "AnyInt"


class _AnyStr(str):
    def __eq__(self, other: object) -> bool:
        if isinstance(other, str):
            return True
        return NotImplemented

    def __hash__(self) -> int:
        return id(ANY_STR)

    def __str__(self) -> str:
        return "AnyStr"

    def __repr__(self) -> str:
        return "AnyStr"


class _Anything(str):
    def __eq__(self, other: object) -> bool:
        return True

    def __hash__(self) -> int:
        return id(ANYTHING)

    def __str__(self) -> str:
        return "Anything"

    def __repr__(self) -> str:
        return "Anything"


ANY_INT = _AnyInt()
ANY_STR = _AnyStr()
ANYTHING = _Anything()

SINGLETONS = {ANY_STR, ANY_INT, ANYTHING}
