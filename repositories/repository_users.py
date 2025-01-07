from typing import List

from pydantic import EmailStr

from db.sqlitedict import SqliteDict
from models.user import User

db = SqliteDict('db/users.db', autocommit=True)


async def create_user(user: User):
    db[user.email] = user


async def read_user_by_email(email: EmailStr) -> User | None:
    return db.get(email, None)
