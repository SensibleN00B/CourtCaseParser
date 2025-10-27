from sqlalchemy import Date, Integer, String, Text
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class Case(Base):
    __tablename__ = "cases"

    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    court_name: Mapped[str] = mapped_column(String(255))
    case_number: Mapped[str] = mapped_column(String(255), unique=True, index=True)
    case_proc: Mapped[str] = mapped_column(String(255), nullable=True)
    registration_date: Mapped[Date] = mapped_column(Date, nullable=True)
    judge: Mapped[str] = mapped_column(String(255), nullable=True)
    judges: Mapped[str] = mapped_column(Text, nullable=True)
    participants: Mapped[str] = mapped_column(Text, nullable=True)
    stage_date: Mapped[Date] = mapped_column(Date, nullable=True)
    stage_name: Mapped[str] = mapped_column(String(255), nullable=True)
    cause_result: Mapped[str] = mapped_column(Text, nullable=True)
    cause_dep: Mapped[str] = mapped_column(String(255), nullable=True)
    type: Mapped[str] = mapped_column(String(255), nullable=True)
    description: Mapped[str] = mapped_column(Text, nullable=True)
