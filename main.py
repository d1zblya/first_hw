import uvicorn
from fastapi import FastAPI

from controllers.controller_books import router as books_router

from loguru import logger
logger.add("file_main.log")

app = FastAPI()

app.include_router(books_router)

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, workers=2)
