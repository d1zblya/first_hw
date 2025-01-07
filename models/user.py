from pydantic import BaseModel, EmailStr


class User(BaseModel):
    user_key: str
    name: str
    email: str
    password: str
    age: int
