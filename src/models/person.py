from datetime import date
from typing import List, Optional

from pydantic import BaseModel


class RelatedPersonMovie(BaseModel):
    """Model to represent movie in which the person took part."""

    id: str
    role: str


class Person(BaseModel):
    """Model to represent Person objects."""

    id: str
    full_name: str
    birth_date: Optional[date]
    related_movies: Optional[List[RelatedPersonMovie]]
