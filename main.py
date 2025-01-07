import uvicorn
from fastapi import FastAPI
from starlette.middleware.cors import CORSMiddleware

from controllers.controller_books import router as books_router
from controllers.controller_users import router as users_router

from loguru import logger

app = FastAPI()

app.include_router(books_router)
app.include_router(users_router)

origins = [
    "http://localhost:8000",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "PATCH", "OPTIONS"],
    allow_headers=["Set-Cookie", "Authorization", "Access-Control-Allow-Origin", "Access-Control-Allow-Headers",
                   "Content-Type"]
)

if __name__ == "__main__":
    logger.add("file_main.log", retention="7 days")
    uvicorn.run("main:app")
