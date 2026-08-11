import uuid


def generate_variable_name() -> str:
    return f"v_{uuid.uuid4().hex}"
