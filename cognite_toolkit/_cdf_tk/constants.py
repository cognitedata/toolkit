try:
    from pyodide.ffi import IN_BROWSER  # type: ignore [import-not-found]
except ModuleNotFoundError:
    IN_BROWSER = False

_RUNNING_IN_BROWSER = IN_BROWSER
