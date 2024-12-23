from typing import List
from uuid import UUID, uuid4

from fastapi import APIRouter

from models.author import Author
from models.book import Book
from models.style import Style
from services import service_books

router = APIRouter(
    prefix="/books",
    tags=["Книги"],
)


@router.post("/create_book")
async def create_book(title: str, author: Author, year: int, style: Style):
    await service_books.create_book(
        Book(book_key=str(uuid4()), title=title, author=author, year=year, style=style),
    )


@router.get("/book/{book_key}")
async def read_book(book_key: str) -> Book:
    return await service_books.read_book_by_book_key(book_key)


@router.get("/all_books")
async def read_all_books() -> List[Book]:
    return await service_books.read_all_books()


@router.get("/all_books_by_style")
async def read_all_books_by_style(style: Style) -> List[Book]:
    return await service_books.get_all_books_by_style(style)


@router.put("/{book_key}/update")
async def update_book(book_key: str, title: str, author: Author, year: int):
    return await service_books.update_book_by_book_key(
        book_key=book_key,
        book=Book(book_key=book_key, title=title, author=author, year=year)
    )


@router.delete("/{book_key}/delete")
async def delete_book(book_key: str):
    await service_books.delete_book_by_book_key(book_key)


@router.delete("/delete/all_books")
async def delete_all_books():
    await service_books.delete_all_books()
