import os
from dotenv import load_dotenv
import psycopg2


def get_connection():
    load_dotenv("configs/.env")

    conn = psycopg2.connect(
        host=os.getenv("DB_HOST"),
        port=os.getenv("DB_PORT"),
        dbname=os.getenv("DB_NAME"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
    )

    return conn
