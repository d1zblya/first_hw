import uuid

from fastapi import APIRouter, Depends, Response
from pydantic import EmailStr

from models.user import User

from services import service_auth

router = APIRouter(
    prefix="/auth",
    tags=["Аутенфикация"],
)


@router.post("/register")
async def register_user(name: str, password: str, email: EmailStr, age: int):
    await service_auth.register_user(
        User(
            user_key=str(uuid.uuid4()),
            name=name,
            password=password,
            email=email,
            age=age
        )
    )


@router.post("/login")
async def login_user(response: Response, email: EmailStr, password: str):
    await service_auth.login_user(response=response, email=email, password=password)


@router.post("/logout")
async def logout_user(response: Response):
    response.delete_cookie("access_token")
