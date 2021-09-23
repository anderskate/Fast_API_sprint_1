from datetime import date
from typing import Optional

from .base import Base


class RelatedPersonMovie(Base):
    """Model to represent movie in which the person took part."""

    id: str
    role: str


class Person(Base):
    """Model to represent Person objects."""

    id: str
    full_name: str
    birth_date: Optional[date]
    related_movies: Optional[list[RelatedPersonMovie]]
