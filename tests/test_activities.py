from sqlalchemy import select
from sqlalchemy.orm import Session

from db.connection import engine
from db.models import Observation


def test_activities():
    with Session(engine) as session:

        observations = session.scalars(
            select(Observation).limit(5)
        ).all()

        for observation in observations:
            print(f"Record ID: {observation.record_id}")
            print("Activities:")

            for item in observation.observation_activities:
                print(f"- {item.activity.activity_name}")

            print("-" * 30)


if __name__ == "__main__":
    test_activities()