from concurrent.futures import ThreadPoolExecutor

from cognite.client.utils._concurrency import ConcurrencySettings, TaskExecutor

from cognite_toolkit._cdf_tk.client._constants import DATA_MODELING_MAX_DELETE_WORKERS, DATA_MODELING_MAX_WRITE_WORKERS

_DATA_MODELING_WRITE_THREAD_POOL_EXECUTOR_SINGLETON: ThreadPoolExecutor
_DATA_MODELING_DELETE_THREAD_POOL_EXECUTOR_SINGLETON: ThreadPoolExecutor


class ToolkitConcurrencySettings(ConcurrencySettings):
    """
    Toolkit-specific concurrency settings for CDF Toolkit operations.
    """

    @classmethod
    def get_data_modeling_write_executor(cls) -> TaskExecutor:
        """
        The data modeling write backend has different concurrency limits compared with the rest of CDF.
        Thus, we use a dedicated executor for these endpoints to match the backend requirements.

        Returns:
            TaskExecutor: The data modeling write executor.
        """
        if cls.uses_mainthread():
            return cls.get_mainthread_executor()

        global _DATA_MODELING_WRITE_THREAD_POOL_EXECUTOR_SINGLETON
        try:
            executor = _DATA_MODELING_WRITE_THREAD_POOL_EXECUTOR_SINGLETON
        except NameError:
            # TPE has not been initialized
            executor = _DATA_MODELING_WRITE_THREAD_POOL_EXECUTOR_SINGLETON = ThreadPoolExecutor(
                DATA_MODELING_MAX_WRITE_WORKERS
            )
        return executor

    @classmethod
    def get_data_modeling_delete_executor(cls) -> TaskExecutor:
        """
        The data modeling delete backend has different concurrency limits compared with the rest of CDF.
        Thus, we use a dedicated executor for these endpoints to match the backend requirements.

        Returns:
            TaskExecutor: The data modeling delete executor.
        """
        if cls.uses_mainthread():
            return cls.get_mainthread_executor()

        global _DATA_MODELING_DELETE_THREAD_POOL_EXECUTOR_SINGLETON
        try:
            executor = _DATA_MODELING_DELETE_THREAD_POOL_EXECUTOR_SINGLETON
        except NameError:
            # TPE has not been initialized
            executor = _DATA_MODELING_DELETE_THREAD_POOL_EXECUTOR_SINGLETON = ThreadPoolExecutor(
                DATA_MODELING_MAX_DELETE_WORKERS
            )
        return executor
