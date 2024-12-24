import unittest
from unittest.mock import AsyncMock, patch
from uuid import uuid4

from models.author import Author
from models.style import Style
from services.service_books import create_book, read_book_by_book_key
from exceptions.exceptions import ErrorBookCreation, ErrorBookNotFound
from repositories import repository_books
from models.book import Book


class TestBaseBookService(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.book_key = str(uuid4())
        self.author = Author(
            name="Иван",
            surname="Иванов",
            patronymic="Иванович",
            year_of_birth=1810
        )
        self.book = Book(
            book_key=self.book_key,
            title="Тестовая книга",
            author=self.author,
            year=1920,
            style=Style.NOVEL
        )


class TestCreateBookService(TestBaseBookService):
    def setUp(self):
        super().setUp()

        self.mock_create_book = AsyncMock()
        patcher = patch.object(
            target=repository_books,
            attribute='create_book',
            new=self.mock_create_book
        )
        self.addCleanup(patcher.stop)  # выключает патчер после каждого теста
        patcher.start()

    async def test_create_book_success(self):
        await create_book(self.book)

        self.mock_create_book.assert_awaited_once_with(self.book)

    async def test_create_book_failure(self):
        self.mock_create_book.side_effect = Exception("Database error")

        with self.assertRaises(ErrorBookCreation) as context:
            await create_book(self.book)

        self.assertIn("Failed create book with details", str(context.exception))
        self.mock_create_book.assert_awaited_once_with(self.book)


class TestReadBookService(TestBaseBookService):
    def setUp(self):
        super().setUp()

        self.mock_read_book = AsyncMock()
        patcher = patch.object(
            target=repository_books,
            attribute='read_book_by_book_key',
            new=self.mock_read_book
        )
        self.addCleanup(patcher.stop)  # выключает патчер после каждого теста
        patcher.start()

    async def test_read_book_success(self):
        self.mock_read_book.return_value = self.book

        result = await read_book_by_book_key(self.book_key)
        self.assertEqual(result, self.book)

    async def test_read_book_failure(self):
        self.mock_read_book.return_value = None

        with self.assertRaises(ErrorBookNotFound) as context:
            await read_book_by_book_key(self.book_key)

        self.assertIn(f"Book with UUID {self.book_key} not found", str(context.exception))
        self.mock_read_book.assert_awaited_once_with(self.book_key)


if __name__ == "__main__":
    unittest.main()
