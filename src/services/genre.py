from typing import Optional, List

from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from src.db.elastic import get_elastic
from src.models.genre import Genre


class GenreService:
    """Service for getting data by Genre."""
    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get_by_id(self, genre_id: str) -> Genre:
        """Get Genre data by id."""
        genre = await self._get_genre_from_elastic(genre_id)
        return genre

    async def _get_genre_from_elastic(self, genre_id: str) -> Genre:
        """Get Genre data by id from ElasticSearch."""
        if not await self.elastic.exists('genres', genre_id):
            return None
        genre_data = await self.elastic.get('genres', genre_id)
        return Genre(**genre_data['_source'])

    async def get_all(self) -> Optional[List[Genre]]:
        """Get all Genres data."""
        genres = await self._get_genres_from_elastic()
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
        elastic: AsyncElasticsearch = Depends(get_elastic)
):
    """Get a service for working with Genre data"""
    return GenreService(elastic)
