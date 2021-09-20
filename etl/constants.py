from enum import Enum


class UpdateTypes(Enum):
    """Update types for starting ETL process."""
    GENRES = 'genres'
    MOVIES = 'movies'
    PERSONS = 'persons'
