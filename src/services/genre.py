from typing import Optional, List

from elasticsearch import AsyncElasticsearch
from aioredis import Redis
from fastapi import Depends
from fastapi.encoders import jsonable_encoder

from db.elastic import get_elastic
from db.redis import get_redis
from models.genre import Genre
from .cache import CacheService, get_cache_service


class GenreService:
    """Service for getting data by Genre."""
    def __init__(self, elastic: AsyncElasticsearch, redis: Redis,
                 cache_service: CacheService):
        self.elastic = elastic
        self.redis = redis
        self.cache_service = cache_service

    async def get_by_id(self, genre_id: str) -> Genre:
        """Get Genre data by id."""
        genre = await self.cache_service.get_obj_from_cache(genre_id, Genre)
        if genre:
            return genre

        genre = await self._get_genre_from_elastic(genre_id)
        await self.cache_service.put_obj_to_cache(genre)

        return genre

    async def _get_genre_from_elastic(self, genre_id: str) -> Genre:
        """Get Genre data by id from ElasticSearch."""
        if not await self.elastic.exists('genres', genre_id):
            return None
        genre_data = await self.elastic.get('genres', genre_id)
        return Genre(**genre_data['_source'])

    async def get_all(self) -> Optional[List[Genre]]:
        """Get all Genres data."""
        genres = await self.cache_service.get_objects_from_cache(
            'genres', Genre
        )
        if genres:
            return genres

        genres = await self._get_genres_from_elastic()
        await self.cache_service.put_objects_to_cache(
            jsonable_encoder(genres), 'genres'
        )
        return genres

    async def _get_genres_from_elastic(self) -> Optional[List[Genre]]:
        """Get all Genres data from ElasticSearch."""
        genres_data = await self.elastic.search(index='genres')
        genres = [
            Genre(**genre_data['_source'])
            for genre_data in genres_data['hits']['hits']
        ]
        return genres


async def get_genre_service(
        elastic: AsyncElasticsearch = Depends(get_elastic),
        redis: Redis = Depends(get_redis),
        cache_service: CacheService = Depends(get_cache_service),
):
    """Get a service for working with Genre data"""
    return GenreService(elastic, redis, cache_service)
