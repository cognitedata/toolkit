def mask_secrets(secrets: dict) -> dict:
    return {k: "***" for k in secrets}
