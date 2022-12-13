import json
import os
from typing import Any, Dict
import boto3
import base64
from .logging import get_logger


def get_secret_values(secret_name: str) -> str:
    logger = get_logger()
    with open("handlers/resources/secrets.json") as f:

        secrets = json.load(f)
        if secret_name not in secrets:
            raise ValueError(f"{secret_name} is not configured in the secrets.json")

        value = secrets[secret_name]
        if isinstance(value, str):
            return value
        else:
            return json.dumps(value)
   


def get_news_api_key() -> str:
    secret_str: str = get_secret_values("NewsApiKey")
    j=json.loads(secret_str)
    return j.get("password")


def get_db_connection_info(db_secret_name="DbSecretName") -> Dict[str, Any]:
    secret_str: str = get_secret_values(db_secret_name)
    return json.loads(secret_str)

