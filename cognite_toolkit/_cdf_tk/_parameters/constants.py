BASE_TYPES = {t.__name__ for t in (str, int, float, bool)}
CONTAINER_TYPES = {t.__name__ for t in (list, dict)}
TYPES = BASE_TYPES | CONTAINER_TYPES
