import datetime
from dataclasses import dataclass, field
from datetime import date
from typing import List, Optional, Set


@dataclass
class RelatedPersonMovie:
    """Representation of related person movie with role in this movie."""
    id: str
    role: str

    @classmethod
    def get_related_movie_from_dict(cls, person_dict) -> 'RelatedPersonMovie':
        """Get RelatedPersonMovie object from dict data."""
        return cls(
            id=person_dict.get('film_work_id'),
            role=person_dict.get('role'),
        )


@dataclass
class Person:
    """Representation of Person object."""
    id: str
    full_name: str
    role: str
    modified: datetime
    birth_date: date = None
    related_movies: List[RelatedPersonMovie] = field(default_factory=list)

    @classmethod
    def get_person_from_dict(cls, movie_dict) -> 'Person':
        """Get Person object from movie dict data."""
        return cls(
            id=movie_dict['id'],
            full_name=movie_dict['full_name'],
            role=movie_dict.get('role'),
            birth_date=movie_dict.get('birth_date'),
            modified=movie_dict.get('modified')
        )

    def get_format_for_es(self) -> list:
        """Get person data for ElasticSearch format structure."""

        meta_data = {"index": {"_index": "persons", "_id": self.id}}
        movie_for_es = {
            'id': self.id,
            'full_name': self.full_name,
            'birth_date': self.birth_date,
            'related_movies': [
                {'id': movie.id, 'role': movie.role}
                for movie in self.related_movies
            ]
        }
        return [meta_data, movie_for_es]


@dataclass
class Genre:
    """Representation of Genre object."""
    id: str
    name: str
    description: Optional[str] = None
    modified: datetime = None

    @classmethod
    def get_genre_from_dict(cls, genre_dict) -> 'Genre':
        """Get Genre object from movie dict data."""
        return cls(
            id=genre_dict['id'],
            name=genre_dict['name'],
            description=genre_dict.get('description'),
            modified=genre_dict.get('modified')
        )

    def get_format_for_es(self) -> list:
        """Get genre data for ElasticSearch format structure."""

        meta_data = {"index": {"_index": "genres", "_id": self.id}}
        genre_for_es = {
            'id': self.id,
            'name': self.name,
            'description': self.description,
        }
        return [meta_data, genre_for_es]


@dataclass
class Movie:
    """Representation of Movie object."""
    id: str
    modified: datetime
    imdb_rating: float
    title: str
    description: str
    directors: List[Person] = field(default_factory=list)
    actors: List[Person] = field(default_factory=list)
    writers: List[Person] = field(default_factory=list)
    genres: List[Genre] = field(default_factory=list)

    def append_person(self, person):
        """Add person depending on role type."""
        directors_ids = [director.id for director in self.directors]
        writers_ids = [writer.id for writer in self.writers]
        actors_ids = [actor.id for actor in self.actors]
        persons_ids = actors_ids + writers_ids + directors_ids
        if person.id in persons_ids:
            return
        if person.role == 'director':
            self.directors.append(person)
        if person.role == 'writer':
            self.writers.append(person)
        if person.role == 'actor':
            self.actors.append(person)

    @classmethod
    def get_movie_from_dict(cls, movie_dict) -> 'Movie':
        """Get Movie object from movie dict data."""
        new_movie = cls(
            id=movie_dict['fw_id'],
            modified=movie_dict['modified'],
            imdb_rating=movie_dict['rating'],
            title=movie_dict['title'],
            description=movie_dict['description'],
        )
        person = Person.get_person_from_dict(movie_dict)
        new_movie.append_person(person)
        genre = Genre(id=movie_dict['genre_id'], name=movie_dict['name'])
        new_movie.genres.append(genre)

        return new_movie

    def get_format_for_es(self) -> list:
        """Get movie data for ElasticSearch format structure."""
        actors_names = ', '.join([actor.full_name for actor in self.actors])
        writers_names = ', '.join([writer.full_name for writer in self.writers])
        actors = [
            {'id': actor.id, 'full_name': actor.full_name}
            for actor in self.actors
        ]
        writers = [
            {'id': writer.id, 'full_name': writer.full_name}
            for writer in self.writers
        ]
        directors = [
            {'id': director.id, 'full_name': director.full_name}
            for director in self.directors
        ]
        genres = [
            {'id': genre.id, 'name': genre.name}
            for genre in self.genres
        ]

        meta_data = {"index": {"_index": "movies", "_id": self.id}}
        movie_for_es = {
            'id': self.id,
            'imdb_rating': self.imdb_rating,
            'title': self.title,
            'description': self.description,
            'genres': genres,
            'actors_names': actors_names,
            'writers_names': writers_names,
            'directors': directors,
            'actors': actors,
            'writers': writers,
        }
        return [meta_data, movie_for_es]
