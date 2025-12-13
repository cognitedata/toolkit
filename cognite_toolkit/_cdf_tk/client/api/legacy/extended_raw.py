from cognite.client._api.raw import RawAPI

from cognite_toolkit._cdf_tk.client.data_classes.raw import RawProfileResults, RawTable


class ExtendedRawAPI(RawAPI):
    DEFAULT_PROFILE_LIMIT = 1000
    MAX_PROFILE_LIMIT = 1_000_000

    def profile(
        self,
        database: str | RawTable,
        table: str | None = None,
        limit: int = DEFAULT_PROFILE_LIMIT,
        timeout_seconds: int | None = None,
    ) -> RawProfileResults:
        """Profiles a table in the specified database and returns the results.

        This is a hidden endpoint that is not part of the official CDF API. However, it is used by the Fusion UI
        to profile tables in the database. This is implemented internally in Cognite Toolkit as Toolkit offers
        profiling of raw tables. This is used to show how data flows into CDF resources.

        Args:
            database (str): The name of the database to profile.
            table (str): The name of the table to profile.
            limit (int, optional): The maximum number of rows to profile. Defaults to DEFAULT_PROFILE_LIMIT.
            timeout_seconds (int, optional): The timeout for the profiling operation in seconds. Defaults to global_config.timeout_seconds.

        Returns:
            RawProfileResults: The results of the profiling operation.

        """
        if limit <= 0 or limit > self.MAX_PROFILE_LIMIT:
            raise ValueError(f"Limit must be between 1 and {self.MAX_PROFILE_LIMIT}, got {limit}.")
        if isinstance(database, RawTable):
            db_name = database.db_name
            table_name = database.table_name
        elif table is None:
            raise ValueError(f"Table name must be provided for {database}.")
        else:
            db_name = database
            table_name = table
        response = self._do_request(
            "POST",
            "/profiler/raw",
            json={"database": db_name, "table": table_name, "limit": limit},
            timeout=timeout_seconds if timeout_seconds is not None else self._config.timeout,
        )
        return RawProfileResults._load(response.json())
