from sqlalchemy import (
    Integer,
    String,
    Numeric,
    ForeignKey,
    CheckConstraint
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.models.base import Base


class Observation(Base):
    __tablename__ = "observation"

    __table_args__ = (
        CheckConstraint(
            "loss_percentage >= 0 AND loss_percentage <= 100",
            name="check_loss_percentage"
        ),
        {"schema": "food_waste"}
    )

    record_id: Mapped[int] = mapped_column(
        Integer,
        primary_key=True
    )

    year: Mapped[int] = mapped_column(
        Integer,
        nullable=False
    )

    loss_percentage: Mapped[float] = mapped_column(
        Numeric(8, 3),
        nullable=False
    )

    cpc_code: Mapped[str] = mapped_column(
        String(30),
        ForeignKey("food_waste.commodity.cpc_code"),
        nullable=False
    )

    method_id: Mapped[int | None] = mapped_column(
        Integer,
        ForeignKey("food_waste.method.method_id"),
        nullable=True
    )

    m49_code: Mapped[str] = mapped_column(
        String(3),
        ForeignKey("food_waste.country.m49_code"),
        nullable=False
    )

    source_id: Mapped[int] = mapped_column(
        Integer,
        ForeignKey("food_waste.source.source_id"),
        nullable=False
    )

    country: Mapped["Country"] = relationship(
        back_populates="observations"
    )
    commodity: Mapped["Commodity"] = relationship(
        back_populates="observations"
    )
    method: Mapped["Method | None"] = relationship(
        back_populates="observations"
    )
    source: Mapped["Source"] = relationship(
        back_populates="observations"
    )
    observation_activities: Mapped[list["ObservationActivity"]] = relationship(
        back_populates="observation"
    )