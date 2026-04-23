from __future__ import annotations

from pathlib import Path
from typing import List
from dotenv import load_dotenv
from pydantic_settings import BaseSettings, SettingsConfigDict

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(dotenv_path=BASE_DIR / ".env")


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=str(BASE_DIR / ".env"),
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    GROQ_API_KEY: str = ""
    GROQ_MODEL: str = "llama-3.1-8b-instant"

    SQL_SERVER: str = r"DESKTOP-9GS64VK\SQLPFE"
    SQL_DRIVER: str = "ODBC Driver 17 for SQL Server"
    SQL_TRUSTED: bool = True
    SQL_USER: str = ""
    SQL_PASSWORD: str = ""

    SNAPSHOT_DIR: str = str(BASE_DIR / "snapshots")

    SSAS_SERVER: str = r"DESKTOP-9GS64VK\SQLPFE"

    # Base SQL Server pour stocker les cubes créés
    REGISTRY_DB: str = "BI_AGENT_DB"

    DWS: List = [
        {
            "id": "dw_sujet1",
            "label": "Sujet 1 - Product Performance",
            "ssas_database": "AdventureWorks_DW_Sujet1_V1",
            "database": "AdventureWorks_DW_Sujet1_V1",
            "schema": "dbo",
            "cube_name": "SSAS_Sujet1_V1",
            "sql_server": "DESKTOP-9GS64VK\\SQLPFE",
            "sql_user": "ssas_dw_reader",
            "sql_password": "Pfe@2026!Strong",
            "provider": "MSOLEDBSQL.1",
        },
        {
            "id": "dw_sujet2",
            "label": "Sujet 2",
            "database": "AdventureWorks_DW_Sujet2_V1",
            "ssas_database": "AdventureWorks_DW_Sujet2_V1",
            "cube_name": "SSAS_Sujet2_V1",
            "schema": "dbo",
            "sql_server": "DESKTOP-9GS64VK\\SQLPFE",
            "sql_user": "ssas_dw_reader",
            "sql_password": "Pfe@2026!Strong",
            "provider": "MSOLEDBSQL.1",
        },
    ]


settings = Settings()