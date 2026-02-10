from pydantic import BaseModel


class ReadModule(BaseModel):
    @property
    def is_success(self) -> bool:
        return True
