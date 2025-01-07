from typing import List
from uuid import UUID

from pydantic import EmailStr

from exceptions.exceptions import UserAlreadyExists, ErrorUserRead, InCorrectPasswordOrEmail, ErrorUserAuthenticate
from repositories import repository_users
from models.user import User
from utils.auth import get_password_hash, authenticate_user, create_access_token

from fastapi import Response

from loguru import logger


async def register_user(user: User):
    try:
        existing_user = await repository_users.read_user_by_email(user.email)
    except Exception as e:
        logger.error(e)
        raise ErrorUserRead(f"Couldn't find the user by email, detail ---> {str(e)}")
    if existing_user:
        msg = "User already exists"
        logger.error(msg)
        raise UserAlreadyExists(msg)
    user.password = get_password_hash(user.password)
    await repository_users.create_user(user)


async def login_user(response: Response, email: EmailStr, password: str):
    try:
        user = await authenticate_user(email, password)
    except Exception as e:
        logger.error(e)
        raise ErrorUserAuthenticate("Authentication error, detail ---> " + str(e))
    if user is None:
        raise InCorrectPasswordOrEmail("Incorrect email or password")
    access_token = create_access_token({"sub": user.user_key})
    response.set_cookie("access_token", access_token, httponly=True)
