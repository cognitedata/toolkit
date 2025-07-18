import functools
import platform

from cognite_toolkit import _version


def get_current_toolkit_version() -> str:
    return _version.__version__


@functools.lru_cache(maxsize=1)
def get_user_agent() -> str:
    toolkit_version = f"CogniteToolkit/{get_current_toolkit_version()}"
    python_version = (
        f"{platform.python_implementation()}/{platform.python_version()} "
        f"({platform.python_build()};{platform.python_compiler()})"
    )
    os_version_info = [platform.release(), platform.machine(), platform.architecture()[0]]
    os_version_info = [s for s in os_version_info if s]  # Ignore empty strings
    os_version_info_str = "-".join(os_version_info)
    operating_system = f"{platform.system()}/{os_version_info_str}"

    return f"{toolkit_version} {python_version} {operating_system}"
