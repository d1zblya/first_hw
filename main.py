from fastapi import FastAPI

from controllers.controller_books import router as books_router

from loguru import logger
logger.add("file_main.log")

app = FastAPI()

app.include_router(books_router)
