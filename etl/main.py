import argparse
import datetime
import os
from abc import ABCMeta, abstractmethod

import redis
from loguru import logger

from constants import UpdateTypes
from elastic_loader import (load_genres_to_es, load_movies_to_es,
                            load_persons_to_es)
from postgres_extractor import PostgresMoviesExtractor
from state_storage import RedisStateStorage
from transform_entities import Genre, Movie, Person, RelatedPersonMovie


class BaseETLFromPostgresToES(metaclass=ABCMeta):
    """Base class to create ETL process from Postgres to ElasticSearch."""

    def __init__(self, **kwargs):
        self.update_time = kwargs.get('update_time')
        self.update_type = kwargs.get('update_type')
        self.redis_key = kwargs.get('redis_key')
        self.state_storage = RedisStateStorage(redis.Redis())

    def __call__(self, *args, **kwargs):
        """Call ETL process."""
        load = self.load()
        load.send(None)
        transform = self.transform(load)
        transform.send(None)
        self.extract(transform)

    @abstractmethod
    def transform(self, loader):
        """Transform data for ElasticSearch."""
        pass

    @abstractmethod
    def extract(self, transformer):
        """Extract data from Postgres."""
        pass

    @abstractmethod
    def load(self, max_batch_size=100):
        """Load data to ElasticSearch."""
        pass


class ETLMoviesFromPostgresToES(BaseETLFromPostgresToES):
    """ETL for load movies data from postgres to elasticsearch."""

    def transform(self, loader):
        """Transform movies data to specific structure
        for further load to elasticsearch."""
        current_movie = None
        while True:
            try:
                movie_data = (yield)
            except GeneratorExit:
                loader.close()
                raise

            movie = Movie.get_movie_from_dict(movie_data)

            if not current_movie:
                current_movie = movie
                continue

            if current_movie and current_movie.id == movie.id:
                person = Person.get_person_from_dict(movie_data)
                current_movie.append_person(person)
                genre = Genre(
                    id=movie_data['genre_id'], name=movie_data['name']
                )
                genres_ids = [genre.id for genre in current_movie.genres]
                if genre.id not in genres_ids:
                    current_movie.genres.append(genre)
                continue

            if current_movie and current_movie.id != movie.id:
                logger.info(f'Transformed movie for ES: \n {current_movie}')
                loader.send(current_movie)
                current_movie = movie

    def extract(self, transformer):
        """Extract movies data from postgres."""
        state_time_for_start = self.state_storage.retrieve_state(
            self.redis_key
        )
        if not state_time_for_start:
            state_time_for_start = self.update_time

        movies_extractor = PostgresMoviesExtractor(
            state_time_for_start, self.update_type
        )
        try:
            movies = movies_extractor.get_movies()
            for row in movies:
                logger.info(f'Extracted movie row: \n {row}')
                transformer.send(row)
        finally:
            movies_extractor.connection.close()
            transformer.close()

    def load(self, max_batch_size=100):
        """Load transformed movies data to elasticsearch."""
        es_host = os.getenv('ES_HOST', 'localhost')
        es_port = os.getenv('ES_PORT', 9200)

        batch_of_movies = []
        last_updated_time = None
        while True:
            try:
                movie = (yield)
            except GeneratorExit:
                if batch_of_movies:
                    load_movies_to_es(es_host, es_port, batch_of_movies)
                    logger.info(f'Load movies to ES: \n {batch_of_movies}')
                self.state_storage.delete_state(self.redis_key)
                logger.info('Load to ES finished!')
                return

            if movie:
                last_updated_time = movie.modified
                formatted_movie = movie.get_format_for_es()
                batch_of_movies += formatted_movie

            if len(batch_of_movies) >= max_batch_size:
                logger.info(f'Load movies to ES: \n {batch_of_movies}')
                load_movies_to_es(es_host, es_port, batch_of_movies)
                self.state_storage.save_state(
                    last_updated_time, self.redis_key
                )
                batch_of_movies = []


class ETLPersonsFromPostgresToES(BaseETLFromPostgresToES):
    """ETL for load persons data from postgres to elasticsearch."""

    def transform(self, loader):
        """Transform persons data to specific structure
        for further load to elasticsearch."""
        current_person = None
        while True:
            try:
                person_data = (yield)
            except GeneratorExit:
                loader.close()
                raise

            person = Person.get_person_from_dict(person_data)

            if not current_person:
                current_person = person

            related_movie = RelatedPersonMovie.get_related_movie_from_dict(
                person_data
            )
            if current_person.id == person.id:
                current_person.related_movies.append(related_movie)
            else:
                logger.info(f'Transformed person for ES: \n {current_person}')
                loader.send(current_person)
                current_person = person
                current_person.related_movies.append(related_movie)

    def extract(self, transformer):
        """Extract persons data from postgres."""
        state_time_for_start = self.state_storage.retrieve_state(
            self.redis_key
        )
        if not state_time_for_start:
            state_time_for_start = self.update_time

        extractor = PostgresMoviesExtractor(
            state_time_for_start
        )
        try:
            persons = extractor.get_updated_persons()
            for row in persons:
                logger.info(f'Extracted person row: \n {row}')
                transformer.send(row)
        finally:
            extractor.connection.close()
            transformer.close()

    def load(self, max_batch_size=100):
        """Load transformed persons data to elasticsearch."""
        es_host = os.getenv('ES_HOST', 'localhost')
        es_port = os.getenv('ES_PORT', 9200)

        batch_of_persons = []
        last_updated_time = None
        while True:
            try:
                person = (yield)
            except GeneratorExit:
                if batch_of_persons:
                    load_persons_to_es(es_host, es_port, batch_of_persons)
                    logger.info(f'Load persons to ES: \n {batch_of_persons}')
                self.state_storage.delete_state(self.redis_key)
                logger.info('Load to ES finished!')
                return

            if person:
                last_updated_time = person.modified
                formatted_person = person.get_format_for_es()
                batch_of_persons += formatted_person

            if len(batch_of_persons) >= max_batch_size:
                logger.info(f'Load persons to ES: \n {batch_of_persons}')
                load_persons_to_es(es_host, es_port, batch_of_persons)
                self.state_storage.save_state(
                    last_updated_time, self.redis_key
                )
                batch_of_persons = []


class ETLGenresFromPostgresToES(BaseETLFromPostgresToES):
    """ETL for load genres data from postgres to elasticsearch."""

    def transform(self, loader):
        """Transform genres data to specific structure
        for further load to elasticsearch."""
        while True:
            try:
                genre_data = (yield)
            except GeneratorExit:
                loader.close()
                raise

            genre = Genre.get_genre_from_dict(genre_data)
            logger.info(f'Transformed genre for ES: \n {genre}')
            loader.send(genre)

    def extract(self, transformer):
        """Extract genres data from postgres."""

        state_time_for_start = (
                self.update_time or
                self.state_storage.retrieve_state(self.redis_key) or
                datetime.datetime.min
        )

        extractor = PostgresMoviesExtractor(
            state_time_for_start
        )
        try:
            genres = extractor.get_updated_genres()
            for row in genres:
                logger.info(f'Extracted genre row: \n {row}')
                transformer.send(row)
        finally:
            extractor.connection.close()
            transformer.close()

    def load(self, max_batch_size=100):
        """Load transformed genres data to elasticsearch."""

        es_host = os.getenv('ES_HOST', 'localhost')
        es_port = os.getenv('ES_PORT', 9200)

        batch_of_genres = []
        last_updated_time = None
        while True:
            try:
                genre = (yield)
            except GeneratorExit:
                if batch_of_genres:
                    load_genres_to_es(es_host, es_port, batch_of_genres)
                    logger.info(f'Load genres to ES: \n {batch_of_genres}')
                logger.info('Load to ES finished!')
                return

            if genre:
                last_updated_time = genre.modified
                formatted_genre = genre.get_format_for_es()
                batch_of_genres += formatted_genre

            if len(batch_of_genres) >= max_batch_size:
                logger.info(f'Load genres to ES: \n {batch_of_genres}')
                load_genres_to_es(es_host, es_port, batch_of_genres)
                self.state_storage.save_state(last_updated_time, self.redis_key)
                batch_of_genres = []


def parse() -> tuple:
    parser = argparse.ArgumentParser(
        description='Script to load movies data from Postgres to ElasticSearch'
    )
    parser.add_argument(
        '--update_time',
        help='DateTime since the beginning of the updated films.'
             'Format: 2021-08-12-21:00',
        type=lambda t: datetime.datetime.strptime(t, '%Y-%m-%d-%H:%M'),
        required=False
    )
    parser.add_argument(
        '--update_type',
        type=UpdateTypes,
        choices=list(UpdateTypes),
        required=True
    )
    args = parser.parse_args()
    return args.update_type.value, args.update_time


def start_etl(update_type, update_time=None):
    etl_processes_map = {
        UpdateTypes.MOVIES.value: [
            ETLMoviesFromPostgresToES(
                update_time=update_time,
                update_type=UpdateTypes.MOVIES.value,
                redis_key='etl_movies_ts',)
        ],
        UpdateTypes.PERSONS.value: [
            ETLPersonsFromPostgresToES(
                update_time=update_time,
                redis_key='etl_persons_ts',
            ),
            ETLMoviesFromPostgresToES(
                update_time=update_time,
                update_type=UpdateTypes.PERSONS.value,
                redis_key='etl_movies_ts',
            )
        ],
        UpdateTypes.GENRES.value: [
            ETLGenresFromPostgresToES(
                update_time=update_time,
                redis_key='etl_genres_ts',
            ),
            ETLMoviesFromPostgresToES(
                update_time=update_time,
                update_type=UpdateTypes.GENRES.value,
                redis_key='etl_movies_ts',
            )
        ]
    }

    for etl in etl_processes_map.get(update_type):
        etl()


if __name__ == '__main__':
    update_type, update_time = parse()
    start_etl(update_type, update_time)
