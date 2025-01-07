from typing import List
from uuid import UUID

from exceptions.exceptions import ErrorBookNotFound, ErrorBookCreation, ErrorBookUpdate, ErrorBookDelete
from models.style import Style
from repositories import repository_books
from models.book import Book

from loguru import logger


async def create_book(book: Book):
    try:
        await repository_books.create_book(book)
        logger.info(f"Book created with details: {book}")
    except Exception as e:
        msg = f"Failed create book with details: {book} ---> Error: {str(e)}"
        logger.error(msg)
        raise ErrorBookCreation(msg)
        # ErrorBookNotFound


async def read_book_by_book_key(book_key: str) -> Book | None:
    book = await repository_books.read_book_by_book_key(book_key)
    if book is None:
        msg = f"Book with UUID {book_key} not found"
        logger.error(msg)
        raise ErrorBookNotFound(msg)
    logger.info(f"Book with UUID {book_key} found")
    return book


async def read_all_books() -> List[Book] | None:
    books = await repository_books.read_all_books()
    if books is None:
        msg = "Not a single book was found"
        logger.error(msg)
        raise ErrorBookNotFound(msg)
    logger.info(f"Found {len(books)} books")
    return books


async def update_book_by_book_key(book_key: str, book: Book):
    try:
        await repository_books.update_book_by_book_key(book_key=book_key, book=book)
        logger.info(f"Book successfully updated with details: {book}")
    except Exception as e:
        msg = f"Failed update book with details: {book} ---> Error: {str(e)}"
        logger.error(msg)
        raise ErrorBookUpdate(msg)


async def delete_book_by_book_key(book_key: str):
    try:
        await repository_books.delete_book_by_book_key(book_key)
        logger.info(f"Book with UUID {book_key} successfully delete")
    except Exception as e:
        msg = f"Failed delete book with details: {book_key} ---> Error: {str(e)}"
        logger.error(msg)
        raise ErrorBookDelete(msg)


async def delete_all_books():
    try:
        await repository_books.delete_all_books()
        logger.info(f"All books successfully deleted")
    except Exception as e:
        msg = f"Failed delete all books ---> Error: {str(e)}"
        logger.error(msg)
        raise ErrorBookDelete(msg)


async def read_all_books_by_style(style: Style) -> List[Book] | None:
    books_by_style = await repository_books.read_all_books()
    if books_by_style is None:
        msg = f"Not a single book was found"
        logger.error(msg)
        raise ErrorBookNotFound(msg)
    books = [book for book in books_by_style if book.style == style]
    logger.info(f"Found {len(books)} books")
    return books
