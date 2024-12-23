from typing import List

from db.sqlitedict import SqliteDict
from models.book import Book

db = SqliteDict('db/books.db', autocommit=True)


async def create_book(book: Book):
    db[book.book_key] = book


async def read_book_by_book_key(book_key: str) -> Book | None:
    return db.get(book_key)


async def read_all_books() -> List[Book] | None:
    return list(db.values())


async def update_book_by_book_key(book_key: str, book: Book):
    db[book_key] = book


async def delete_book_by_book_key(book_key: str):
    del db[book_key]


async def delete_all_books():
    db.clear()
