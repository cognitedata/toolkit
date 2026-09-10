from typing import Literal, TypeAlias

Provider: TypeAlias = Literal["entra_id", "auth0", "cdf", "other"]
LoginFlow: TypeAlias = Literal["client_credentials", "token", "device_code", "interactive", "session"]
