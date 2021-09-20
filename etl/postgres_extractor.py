import os

import backoff
import psycopg2
from loguru import logger
from psycopg2.extras import DictCursor

from constants import UpdateTypes


class PostgresMoviesExtractor:
    """Extractor to get movies data from postgres db."""
    def __init__(
            self, update_time, update_type=UpdateTypes.PERSONS.value):
        dsl = {
            'dbname': os.getenv('DB_POSTGRES', 'django_movies'),
            'user': os.getenv('POSTGRES_USER', 'postgres'),
            'password': os.getenv('POSTGRES_PASSWORD', 'postgres'),
            'host': os.getenv('POSTGRES_HOST', '127.0.0.1'),
            'port': os.getenv('POSTGRES_PORT', 5432),
        }
        self.update_time = update_time
        self.update_type = update_type
        self.dsl = dsl
        self.connection = self.connect_to_db()
        self.cursor = self.connection.cursor()

    @backoff.on_exception(
        backoff.expo, psycopg2.OperationalError,
        max_time=60, logger=logger
    )
    def connect_to_db(self):
        """Connect to postgres db and return connection."""
        return psycopg2.connect(
            **self.dsl, cursor_factory=DictCursor
        )

    def get_updated_persons_ids(self) -> tuple:
        """Get updated persons ids."""
        query = """
                SELECT id, modified
                FROM movies_person
                WHERE modified >= %(datetime)s
                ORDER BY modified
                LIMIT 100;
                """
        params = {'datetime': self.update_time}

        self.cursor.execute(query, params)
        person_ids = [
            person_info[0] for person_info
            in self.cursor.fetchall()
        ]
        return tuple(person_ids)

    def get_updated_persons(self):
        """Get info about updated persons."""
        query = """
                SELECT p.id, p.full_name, p.birth_date, 
                p.modified, pfw.film_work_id, pfw.role
                FROM movies_person p
                LEFT JOIN movies_personfilmwork pfw
                ON pfw.person_id = p.id
                WHERE p.modified >= %(datetime)s
                ORDER BY p.id, p.modified;
                """
        params = {'datetime': self.update_time}
        self.cursor.execute(query, params)

        return (row for row in self.cursor)

    def get_updated_genres(self):
        """Get info about updated persons."""
        query = """
                SELECT id, modified, name, description
                FROM movies_genre
                WHERE modified >= %(datetime)s
                ORDER BY modified;
                """
        params = {'datetime': self.update_time}
        self.cursor.execute(query, params)

        return (row for row in self.cursor)

    def get_film_works_ids(self, updated_persons_ids) -> tuple:
        """Get a list of film_works starring the updated persons."""
        query = """
            SELECT fw.id, fw.modified
            FROM movies_filmwork fw
            LEFT JOIN movies_personfilmwork pfw ON pfw.film_work_id = fw.id
            WHERE fw.modified >= %(datetime)s 
            AND pfw.person_id IN %(updated_persons_ids)s
            ORDER BY fw.modified
            LIMIT 100; 
        """
        params = {
            'datetime': self.update_time,
            'updated_persons_ids': updated_persons_ids
        }
        self.cursor.execute(query, params)

        film_works_ids = [
            film_work_info[0] for film_work_info
            in self.cursor.fetchall()
        ]
        return tuple(film_works_ids)

    def get_film_works_info(self, film_works_ids):
        """Get full film_works for getting film_works ids."""
        query = """
            SELECT
            fw.id as fw_id, 
            fw.title, 
            fw.description, 
            fw.rating, 
            fw.type, 
            fw.created, 
            fw.modified, 
            pfw.role, 
            p.id, 
            p.full_name,
            gfw.genre_id,
            g.name
            FROM movies_filmwork fw
            LEFT JOIN movies_personfilmwork pfw ON pfw.film_work_id = fw.id
            LEFT JOIN movies_person p ON p.id = pfw.person_id
            LEFT JOIN movies_filmwork_genres gfw ON gfw.filmwork_id = fw.id
            LEFT JOIN movies_genre g ON g.id = gfw.genre_id
            WHERE fw.id IN %(film_works_ids)s;
        """
        params = {
            'film_works_ids': film_works_ids
        }
        self.cursor.execute(query, params)
        return (row for row in self.cursor)

    def get_updated_genres_ids(self) -> tuple:
        """Get updated genres ids."""
        query = """
            SELECT id, modified
            FROM movies_genre
            WHERE modified >= %(datetime)s
            ORDER BY modified
            LIMIT 100;
        """
        params = {
            'datetime': self.update_time,
        }
        self.cursor.execute(query, params)

        genres_ids = [
            genre_info[0] for genre_info
            in self.cursor.fetchall()
        ]
        return tuple(genres_ids)

    def get_film_works_by_updated_genres(self, genres_ids):
        """Get film_work by updated genres."""
        query = """
            SELECT
            fw.id as fw_id,
            fw.title,
            fw.description,
            fw.rating,
            fw.type,
            fw.created,
            fw.modified,
            pfw.role,
            p.id,
            p.full_name,
            gfw.genre_id,
            g.name
            FROM movies_filmwork fw
            LEFT JOIN movies_personfilmwork pfw ON pfw.film_work_id = fw.id
            LEFT JOIN movies_person p ON p.id = pfw.person_id
            LEFT JOIN movies_filmwork_genres gfw ON gfw.filmwork_id = fw.id
            LEFT JOIN movies_genre g ON g.id = gfw.genre_id
            WHERE g.id IN %(genres_ids)s;
        """
        params = {
            'genres_ids': genres_ids,
        }
        self.cursor.execute(query, params)
        return (row for row in self.cursor)

    def get_updated_movies(self):
        """Get updated movies"""
        query = """
            SELECT
                fw.id as fw_id,
                fw.title,
                fw.description,
                fw.rating,
                fw.type,
                fw.created,
                fw.modified,
                pfw.role,
                p.id,
                p.full_name,
                gfw.genre_id,
                g.name
                FROM movies_filmwork fw
                LEFT JOIN movies_personfilmwork pfw ON pfw.film_work_id = fw.id
                LEFT JOIN movies_person p ON p.id = pfw.person_id
                LEFT JOIN movies_filmwork_genres gfw ON gfw.filmwork_id = fw.id
                LEFT JOIN movies_genre g ON g.id = gfw.genre_id
                WHERE fw.modified >= %(datetime)s
                ORDER BY fw.modified;
        """
        params = {
            'datetime': self.update_time,
        }
        self.cursor.execute(query, params)
        return (row for row in self.cursor)

    def get_movies_for_updated_persons(self):
        """Get movies for updated persons."""
        person_ids = self.get_updated_persons_ids()
        film_works_ids = self.get_film_works_ids(person_ids)
        movies = self.get_film_works_info(film_works_ids)
        return movies

    def get_movies_for_updated_genres(self):
        """Get movies for updated genres."""
        updated_genres_ids = self.get_updated_genres_ids()
        movies = self.get_film_works_by_updated_genres(updated_genres_ids)
        return movies

    @backoff.on_exception(
        backoff.expo, psycopg2.OperationalError,
        max_time=60, logger=logger
    )
    def get_movies(self):
        """Get information on movies depending on the update."""
        update_type_map = {
            UpdateTypes.PERSONS.value:
                self.get_movies_for_updated_persons,
            UpdateTypes.MOVIES.value:
                self.get_updated_movies,
            UpdateTypes.GENRES.value:
                self.get_movies_for_updated_genres
        }
        return update_type_map.get(self.update_type)()
