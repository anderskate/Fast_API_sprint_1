from typing import Optional

from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from src.db.elastic import get_elastic
from src.models.movie import Movie
from src.utils.utils import (get_genres_filter_for_elastic,
                             get_movies_sorting_for_elastic,
                             get_search_body_for_movies, parse_objects)


class MovieService:
    """Service for getting data for movie."""

    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get_by_id(self, movie_id: str) -> Optional[Movie]:
        """Get movie data by id."""
        return await self._get_movie_from_elastic(movie_id)

    async def get_all(
            self, page: int, size: int,
            sort: Optional[str], genres: Optional[str]
    ):
        """Get all movies data with optional filters."""
        return await self._get_movies_from_elastic(page, size, sort, genres)

    async def _get_movies_from_elastic(
            self, page: int, size: int,
            sort: Optional[str], genres: Optional[list[str]]
    ):
        """Get movies from ElasticSearch with optional filters."""
        body = {"size": size, "from": (page - 1) * size}
        if sort:
            body.update(get_movies_sorting_for_elastic(sort))
        if genres:
            body.update(get_genres_filter_for_elastic(genres))
        res = await self.elastic.search(index="movies", body=body)
        return parse_objects(res, Movie)

    async def search_movies(self, page: int, size: int, query: str):
        """Find movies by specific query."""
        movies = await self._search_movie_in_elastic(page, size, query)
        return movies

    async def _search_movie_in_elastic(
            self, page: int, size: int, query: str
    ) -> list[Optional[Movie]]:
        """Search movies in ElasticSearch by specific query."""
        body = {"size": size, "from": (page - 1) * size}
        body.update(get_search_body_for_movies(query))
        res = await self.elastic.search(index="movies", body=body)
        return parse_objects(res, Movie)

    async def _get_movie_from_elastic(self, movie_id: str) -> Optional[Movie]:
        """Get movie data from ElasticSearch."""
        if not await self.elastic.exists("movies", movie_id):
            return None
        movie_data = await self.elastic.get("movies", movie_id)
        return Movie(**movie_data["_source"])


def get_movie_service(
        elastic: AsyncElasticsearch = Depends(get_elastic)) -> MovieService:
    """Get a service for working with Movie data"""
    return MovieService(elastic)
