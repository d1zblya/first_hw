from pydantic import BaseModel


class Author(BaseModel):
    name: str
    surname: str
    patronymic: str
    year_of_birth: int
