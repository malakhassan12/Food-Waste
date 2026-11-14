
from sqlalchemy import Integer, ForeignKey
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.models.base import Base


class ObservationActivity(Base):
    __tablename__ = "observation_activity"
    __table_args__ = {"schema": "food_waste"}

    record_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey(
            "food_waste.observation.record_id",
            ondelete="CASCADE"
        ),
        primary_key=True
    )

    activity_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("food_waste.activity.activity_id"),
        primary_key=True
    )

    observation: Mapped["Observation"] = relationship(
        back_populates="observation_activities"
    )

    activity: Mapped["Activity"] = relationship(
        back_populates="observation_activities"
    )
