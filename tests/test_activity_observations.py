
from sqlalchemy import select
from sqlalchemy.orm import Session

from db.connection import engine
from db.models import Activity


def test_activity_observations():
    with Session(engine) as session:

        activities = session.scalars(
            select(Activity).limit(5)
        ).all()

        for activity in activities:
            print(f"Activity: {activity.activity_name}")
            print("Observations:")

            for item in activity.observation_activities:
                observation = item.observation

                print(
                    f"- Record ID: {observation.record_id}, "
                    f"Year: {observation.year}, "
                    f"Loss: {observation.loss_percentage}%"
                )

            print("-" * 40)


if __name__ == "__main__":
    test_activity_observations()
