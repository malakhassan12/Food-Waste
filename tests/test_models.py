from sqlalchemy import select, func
from sqlalchemy.orm import Session

from db.connection import engine
from db.models import Country, Observation


def test_orm():
    with Session(engine) as session:

        countries = session.scalars(
            select(Country).limit(5)
        ).all()

        print("First 5 Countries:")

        for country in countries:
            print(country.m49_code, country.country_name)

        total_observations = session.scalar(
            select(func.count()).select_from(Observation)
        )

        print("\nObservation Records:", total_observations)


if __name__ == "__main__":
    test_orm()