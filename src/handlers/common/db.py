import os
import json
import sqlalchemy
from typing import Dict, Any
from .logging import get_logger
from .secrets import get_db_connection_info


def get_db_engine(protocol: str = "snowflake", db_secret_name: str = "DbSecretName"):
    
    logger = get_logger()
    logger.info(f"db secret name is {db_secret_name} ")

    secrets: Dict[str, Any] = get_db_connection_info(db_secret_name=db_secret_name)
    db_name: str = secrets.get("dbname", None)
    db_port: str = secrets.get("dbport", 443)
    db_user: str = secrets.get("username", None)
    db_password: str = secrets.get("password", None)
    host: str = secrets.get("host", None)
    warehouse: str = secrets.get("warehouse",None)

    if db_name is None:
        raise ValueError("db_name cannot be None.")

    if db_user is None:
        raise ValueError("db_user cannot be None.")

    if db_password is None:
        raise ValueError("db_password cannot be None.")

    if host is None:
        raise ValueError("host cannot be None.")


    return sqlalchemy.create_engine(f"{protocol}://{db_user}:{db_password}@{host}/interview/de_landing?warehouse=analyst_warehouse&role=interview")
    #I should really hide all of those params but I'm feeling short on time
