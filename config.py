from pydantic.v1 import BaseSettings


class Settings(BaseSettings):
    ALGORITHM: str
    SECRET_KEY: str

    class Config:
        env_file = ".env"


settings = Settings()
