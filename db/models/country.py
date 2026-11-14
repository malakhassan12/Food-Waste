from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.models.base import Base


class Country(Base):
    __tablename__ = "country"
    __table_args__ = {"schema": "food_waste"}

    m49_code: Mapped[str] = mapped_column(
        String(3),
        primary_key=True
    )

    country_name: Mapped[str] = mapped_column(
        String(150),
        nullable=False,
        unique=True
    )

    observations: Mapped[list["Observation"]] = relationship(
        back_populates="country"
    )