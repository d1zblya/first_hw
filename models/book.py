from pydantic import BaseModel

from models.author import Author
from models.style import Style


class Book(BaseModel):
    book_key: str
    title: str
    author: Author
    year: int
    style: Style
