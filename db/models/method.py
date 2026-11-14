from sqlalchemy import Integer, String, Identity
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.models.base import Base


class Method(Base):
    __tablename__ = "method"
    __table_args__ = {"schema": "food_waste"}

    method_id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=False),
        primary_key=True
    )

    method_name: Mapped[str] = mapped_column(
        String(250),
        nullable=False,
        unique=True
    )

    observations: Mapped[list["Observation"]] = relationship(
        back_populates="method"
    )