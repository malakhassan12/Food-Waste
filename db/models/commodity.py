from sqlalchemy import String
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.models.base import Base


class Commodity(Base):
    __tablename__ = "commodity"
    __table_args__ = {"schema": "food_waste"}

    cpc_code: Mapped[str] = mapped_column(
        String(30),
        primary_key=True
    )

    commodity_name: Mapped[str] = mapped_column(
        String(250),
        nullable=False,
        unique=True
    )

    observations: Mapped[list["Observation"]] = relationship(
        back_populates="commodity"
    )