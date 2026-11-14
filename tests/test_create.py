
from sqlalchemy.orm import Session

from db.connection import engine
from db.models import Country


def cleanup():
    with Session(engine) as session:

        country = session.get(Country, "997")

        if country:
            session.delete(country)
            session.commit()
            print("Test record deleted successfully!")
        else:
            print("No test record found.")


if __name__ == "__main__":
    cleanup()
