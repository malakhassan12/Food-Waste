
from sqlalchemy import select, func, case, union, union_all, exists
from sqlalchemy.orm import Session

from db.connection import engine
from db.models import Observation, Country, Commodity, Activity, ObservationActivity


# 1. GROUP BY + HAVING
def get_countries_with_high_loss():
    with Session(engine) as session:

        statement = (
            select(
                Country.country_name,
                func.count(Observation.record_id).label("total_records"),
                func.avg(Observation.loss_percentage).label("average_loss")
            )
            .join(Country.observations)
            .group_by(Country.country_name)
            .having(
                func.count(Observation.record_id) >= 10,
                func.avg(Observation.loss_percentage) > 5
            )
            .order_by(func.avg(Observation.loss_percentage).desc())
        )

        results = session.execute(statement).all()

        print("\nCountries with High Average Food Loss")
        print("-------------------------------------")

        for row in results:
            print(
                f"Country: {row.country_name} | "
                f"Records: {row.total_records} | "
                f"Average Loss: {row.average_loss:.3f}%"
            )


# 2. DISTINCT
def get_distinct_years():
    with Session(engine) as session:

        statement = (
            select(Observation.year)
            .distinct()
            .order_by(Observation.year)
        )

        results = session.scalars(statement).all()

        print("\nDistinct Years")
        print("--------------")

        for year in results:
            print(year)

        print(f"Total Unique Years: {len(results)}")


# 3. SUBQUERY
def get_countries_above_global_average():
    with Session(engine) as session:

        average_subquery = (
            select(func.avg(Observation.loss_percentage))
            .scalar_subquery()
        )

        statement = (
            select(
                Country.country_name,
                func.avg(Observation.loss_percentage).label("average_loss")
            )
            .join(Country.observations)
            .group_by(Country.country_name)
            .having(func.avg(Observation.loss_percentage) > average_subquery)
            .order_by(func.avg(Observation.loss_percentage).desc())
        )

        results = session.execute(statement).all()

        print("\nCountries Above Global Average Loss")
        print("-----------------------------------")

        for row in results:
            print(
                f"{row.country_name}: "
                f"{row.average_loss:.3f}%"
            )


# 4. EXISTS
def get_countries_with_observations():
    with Session(engine) as session:

        observation_exists = (
            select(Observation.record_id)
            .where(Observation.m49_code == Country.m49_code)
            .exists()
        )

        statement = (
            select(Country.country_name)
            .where(observation_exists)
            .order_by(Country.country_name)
        )

        results = session.scalars(statement).all()

        print("\nCountries With Observations")
        print("---------------------------")

        for country in results:
            print(country)

        print(f"Total Countries: {len(results)}")


# 5. UNION
def get_countries_from_two_queries():
    with Session(engine) as session:

        egypt_query = select(Country.country_name).where(
            Country.country_name == "Egypt"
        )

        morocco_query = select(Country.country_name).where(
            Country.country_name == "Morocco"
        )

        combined_query = union(egypt_query, morocco_query)

        results = session.execute(combined_query).all()

        print("\nUNION Results")
        print("-------------")

        for row in results:
            print(row.country_name)


# 6. UNION ALL
def get_countries_using_union_all():
    with Session(engine) as session:

        first_query = select(Country.country_name).where(
            Country.country_name == "Egypt"
        )

        second_query = select(Country.country_name).where(
            Country.country_name == "Egypt"
        )

        combined_query = union_all(first_query, second_query)

        results = session.execute(combined_query).all()

        print("\nUNION ALL Results")
        print("-----------------")

        for row in results:
            print(row.country_name)


# 7. CASE WHEN
def classify_observations():
    with Session(engine) as session:

        loss_category = case(
            (Observation.loss_percentage >= 20, "High Loss"),
            (Observation.loss_percentage >= 10, "Medium Loss"),
            else_="Low Loss"
        ).label("loss_category")

        statement = (
            select(
                Observation.record_id,
                Observation.loss_percentage,
                loss_category
            )
            .order_by(Observation.loss_percentage.desc())
            .limit(20)
        )

        results = session.execute(statement).all()

        print("\nObservation Loss Classification")
        print("--------------------------------")

        for row in results:
            print(
                f"Record: {row.record_id} | "
                f"Loss: {row.loss_percentage}% | "
                f"Category: {row.loss_category}"
            )


# 8. COUNT BY ACTIVITY
def get_activity_statistics():
    with Session(engine) as session:

        statement = (
            select(
                Activity.activity_name,
                func.count(ObservationActivity.record_id).label("total_observations")
            )
            .join(Activity.observation_activities)
            .group_by(Activity.activity_name)
            .order_by(func.count(ObservationActivity.record_id).desc())
        )

        results = session.execute(statement).all()

        print("\nObservations By Activity")
        print("------------------------")

        for row in results:
            print(
                f"Activity: {row.activity_name} | "
                f"Observations: {row.total_observations}"
            )


if __name__ == "__main__":
    get_countries_with_high_loss()
    get_distinct_years()
    get_countries_above_global_average()
    get_countries_with_observations()
    get_countries_from_two_queries()
    get_countries_using_union_all()
    classify_observations()
    get_activity_statistics()
