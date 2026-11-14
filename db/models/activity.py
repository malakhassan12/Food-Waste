from sqlalchemy import Integer, String, Identity
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.models.base import Base


class Activity(Base):
    __tablename__ = "activity"
    __table_args__ = {"schema": "food_waste"}

    activity_id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=False),
        primary_key=True
    )

    activity_name: Mapped[str] = mapped_column(
        String(250),
        nullable=False,
        unique=True
    )

    observation_activities: Mapped[list["ObservationActivity"]] = relationship(
        back_populates="activity"
    )