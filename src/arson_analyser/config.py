from pydantic_settings import BaseSettings


class Config(BaseSettings):
    class Config:
        env_file = ".env"

    nasa_api_key: str = ""
