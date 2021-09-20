import backoff
from elasticsearch import Elasticsearch, exceptions
from loguru import logger


@backoff.on_exception(
    backoff.expo, exceptions.ConnectionError,
    max_time=60, logger=logger,
)
def load_movies_to_es(host, port, movies):
    """Load movies data to ElasticSearch."""
    try:
        es = Elasticsearch([{'host': host, 'port': port}])
        if not es.indices.exists('movies'):
            raise Exception('Index not created in ElasticSearch')
        es.bulk(index='movies', body=movies)
    finally:
        es.close()


def load_persons_to_es(host, port, persons):
    """Load persons data to ElasticSearch."""
    try:
        es = Elasticsearch([{'host': host, 'port': port}])
        if not es.indices.exists('persons'):
            raise Exception('Index not created in ElasticSearch')
        es.bulk(index='persons', body=persons)
    finally:
        es.close()


def load_genres_to_es(host, port, genres):
    """Load genres data to ElasticSearch."""
    try:
        es = Elasticsearch([{'host': host, 'port': port}])
        if not es.indices.exists('genres'):
            raise Exception('Index not created in ElasticSearch')
        es.bulk(index='genres', body=genres)
    finally:
        es.close()
