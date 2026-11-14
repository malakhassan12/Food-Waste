from sqlalchemy import Integer, Identity, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship

from db.models.base import Base


class Source(Base):
    __tablename__ = "source"
    __table_args__ = {"schema": "food_waste"}

    source_id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=False),
        primary_key=True
    )

    reference: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )

    url: Mapped[str | None] = mapped_column(
        Text,
        nullable=True
    )

    observations: Mapped[list["Observation"]] = relationship(
        back_populates="source"
    )