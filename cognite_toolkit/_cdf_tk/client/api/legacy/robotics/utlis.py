import contextlib
from collections.abc import Iterator


@contextlib.contextmanager
def tmp_disable_gzip() -> Iterator[None]:
    from cognite.client.config import global_config

    _current_value = global_config.disable_gzip

    global_config.disable_gzip = True
    yield None
    global_config.disable_gzip = _current_value
