from pydantic import BaseModel, DirectoryPath


class Module(BaseModel):
    path: DirectoryPath

    @property
    def name(self) -> str:
        return self.path.name
