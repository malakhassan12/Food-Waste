
import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

# Use Docker hostname inside containers, localhost from Windows
if os.getenv("RUNNING_IN_DOCKER") != "true":
    DATABASE_URL = DATABASE_URL.replace("@postgres:5432", "@localhost:5433")

# Set food_waste as the default schema
engine = create_engine(
    DATABASE_URL,
    connect_args={
        "options": "-csearch_path=food_waste,public"
    }
)


def test_connection():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("""
                SELECT
                    current_database(),
                    current_schema(),
                    to_regclass('country')
            """))

            row = result.fetchone()

            print("Connected successfully!")
            print(f"Database: {row[0]}")
            print(f"Schema: {row[1]}")
            print(f"Country table: {row[2]}")

    except Exception as error:
        print("Connection failed!")
        print(error)


if __name__ == "__main__":
    test_connection()
