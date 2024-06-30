import psycopg2
from scripts.process_config import DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_HOST, DWH_PORT

def get_db_connection():
    conn = psycopg2.connect(
        dbname=DWH_DB,
        user=DWH_DB_USER,
        password=DWH_DB_PASSWORD,
        host=DWH_HOST,
        port=DWH_PORT
    )
    return conn
