from pathlib import Path


class ModuleLock:
    def __init__(self, module_ids: list[Path]) -> None:
        self.module_ids = module_ids

    def try_lock(self) -> list[Path]:
        """Try to look the modules in the list. Return a list of modules that could not be locked."""
        raise NotImplementedError()

    def release_lock(self) -> None:
        """Release the locks on the modules in the list."""
        raise NotImplementedError()
