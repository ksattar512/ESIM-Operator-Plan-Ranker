"""
Developer Signature: Kashif Sattar — ESIM Operator Plan Ranker
Purpose: Centralized, environment-driven application configuration.
Public Repo Note: Do not hardcode database, SSH, API, or secret values in source code.
"""

import os


class Config:
    """Application configuration loaded from environment variables."""

    APP_NAME = os.getenv("APP_NAME", "ESIM Operator Plan Ranker")
    VERSION = os.getenv("APP_VERSION", "1.0.0")
    DEBUG = os.getenv("DEBUG", "false").lower() == "true"

    DATABASE_URI = os.getenv("DATABASE_URI", "mysql://user:password@localhost:3306/database")
    SECRET_KEY = os.getenv("SECRET_KEY", "change-me")

    HOST_NAME = os.getenv("DB_HOST", "localhost")
    USER_NAME = os.getenv("DB_USER", "root")
    PASSWORD = os.getenv("DB_PASSWORD", "")
    HPASSWORD = os.getenv("DB_HISTORY_PASSWORD", "")
    DB = os.getenv("DB_NAME", "esim_ranker")

    ssh_host = os.getenv("SSH_HOST", "")
    ssh_user = os.getenv("SSH_USER", "")
    ssh_port = int(os.getenv("SSH_PORT", "22"))
    sql_ip = os.getenv("SQL_IP", HOST_NAME)
    SSH_PRIVATE_KEY_PATH = os.getenv("SSH_PRIVATE_KEY_PATH", "")
