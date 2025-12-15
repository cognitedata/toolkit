class EndpointAssertionError(AssertionError):
    """Raised when an endpoint does not behave as expected."""

    def __init__(self, endpoint: str, message: str) -> None:
        super().__init__(f"Endpoint '{endpoint}' failed: {message}")
        self.endpoint = endpoint
        self.message = message
