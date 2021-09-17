from typing import Optional, List

from aioredis import Redis
from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from src.db.elastic import get_elastic
from src.db.redis import get_redis
from src.models.movie import Movie
from src.utils.utils import (get_sort_for_elastic,
                             get_genres_filter_for_elastic,
                             get_search_body_for_movies,
                             parse_objects)


class MovieService:
    def __init__(self, redis: Redis, elastic: AsyncElasticsearch):
        self.redis = redis
        self.elastic = elastic

    async def get_by_id(self, movie_id: str) -> Optional[Movie]:
        return await self._get_movie_from_elastic(movie_id)

    async def get_all(self, sort: Optional[str], genres: Optional[str]):
        return await self._get_movies_from_elastic(sort, genres)

    async def _get_movies_from_elastic(self, sort: Optional[str], genres: Optional[List[str]]):
        body = {}
        body.update(get_sort_for_elastic(sort))
        body.update(get_genres_filter_for_elastic(genres))
        res = await self.elastic.search(index='movies', body=body)
        return parse_objects(res, Movie)

    async def search_movies(self, query):
        movies = await self._search_movie_in_elastic(query)
        return movies

    async def _search_movie_in_elastic(self, query: str) -> List[Optional[Movie]]:
        res = await self.elastic.search(index='movies', body=get_search_body_for_movies(query))
        return parse_objects(res, Movie)

    async def _get_movie_from_elastic(self, movie_id: str) -> Optional[Movie]:
        doc = await self.elastic.get('movies', movie_id)
        return Movie(**doc['_source'])


def get_movie_service(
        redis: Redis = Depends(get_redis),
        elastic: AsyncElasticsearch = Depends(get_elastic)
) -> MovieService:
    return MovieService(redis, elastic)
