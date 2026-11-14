from sqlalchemy import select
from sqlalchemy.orm import Session

from db.connection import engine
from db.models import Country, Commodity, Observation


def test_relationship():
    with Session(engine) as session:

        statement = select(Observation).limit(5)

        observations = session.scalars(statement).all()

        for observation in observations:
            print(f"Record ID: {observation.record_id}")
            print(f"Country: {observation.country.country_name}")
            print(f"Commodity: {observation.commodity.commodity_name}")
            print(f"Method: {observation.method.method_name if observation.method else 'Not specified'}")
            print(f"Source: {observation.source.reference}")
            print("-" * 30)


if __name__ == "__main__":
    test_relationship()