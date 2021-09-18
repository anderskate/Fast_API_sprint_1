from typing import List

import orjson
from pydantic import BaseModel

from .genre import Genre
from .person import Person


class Movie(BaseModel):
    """Model to represent Movie objects."""

    id: str
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


ALLOWED_SORT_FIELDS = {"imdb_rating"}
FIELDS_FOR_SEARCH = ["title", "description", "actors_names", "writers_names"]
