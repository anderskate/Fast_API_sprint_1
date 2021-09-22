from .genre import Genre
from .person import Person
from .base import Base


class Movie(Base):
    """Model to represent Movie objects."""

    id: str
    imdb_rating: float
    title: str
    description: str
    genres: list[Genre]
    actors: list[Person]
    writers: list[Person]
    directors: list[Person]


FIELDS_FOR_SEARCH = ["title", "description", "actors_names", "writers_names"]
