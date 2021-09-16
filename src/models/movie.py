import orjson
from uuid import UUID
from typing import List

from pydantic import BaseModel

from .genre import Genre
from .person import Person


class Movie(BaseModel):
    """Model to represent Movie objects."""
    id: UUID
    imdb_rating: float
    title: str
    description: str
    genres: List[Genre]
    actors: List[Person]
    writers: List[Person]
    directors: List[Person]

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson.dumps
