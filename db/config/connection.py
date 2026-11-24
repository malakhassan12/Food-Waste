import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()

DATABASE_URL = os.getenv("DATABASE_URL")

if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is not set")

engine = create_engine(DATABASE_URL)


def test_connection():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT current_database();"))

            print("Connected successfully!")
            print(f"Database: {result.scalar()}")

    except Exception as error:
        print("Connection failed!")
        print(error)


if __name__ == "__main__":
    test_connection()