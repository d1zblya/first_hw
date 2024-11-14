from dataclasses import dataclass


@dataclass
class Book:
    book_key: str
    title: str
    author: str
    year: int
