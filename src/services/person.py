from typing import Optional

from elasticsearch import AsyncElasticsearch
from fastapi import Depends

from src.db.elastic import get_elastic
from src.models.movie import Movie
from src.models.person import Person
from src.utils.utils import parse_objects


class PersonService:
    """Service for getting data by Person."""

    def __init__(self, elastic: AsyncElasticsearch):
        self.elastic = elastic

    async def get_by_id(self, person_id: str) -> Optional[Person]:
        """Get person data by id."""
        person = await self._get_person_from_elastic(person_id)
        return person

    async def _get_person_from_elastic(
            self, person_id: str
    ) -> Optional[Person]:
        """Get person data from ElasticSearch."""
        if not await self.elastic.exists("persons", person_id):
            return None
        person_data = await self.elastic.get("persons", person_id)
        return Person(**person_data["_source"])

    async def get_person_movies(
            self, page: int, size: int, person_id: str
    ) -> Optional[list[Movie]]:
        """Get all person movies data."""
        person = await self._get_person_from_elastic(person_id)
        if not person:
            return None
        person_movie_ids = [movie.id for movie in person.related_movies]
        return await self._get_person_movies_from_elastic(
            page, size, person_movie_ids
        )

    async def _get_person_movies_from_elastic(
            self, page: int, size: int, movie_ids: list[str]
    ) -> list[Movie]:
        """Get all person movies data from ElasticSearch."""
        body = {
            "size": size, "from": (page - 1) * size,
            "query": {"ids": {"values": movie_ids}}
        }
        person_movies_data = await self.elastic.search(
            index="movies", body=body
        )
        return parse_objects(person_movies_data, Movie)

    async def search_persons(
            self, page: int, size: int, query: str
    ) -> list[Person]:
        """Find persons by specific query."""
        return await self._search_persons_from_elastic(page, size, query)

    async def _search_persons_from_elastic(
            self, page: int, size: int, query: str
    ) -> list[Person]:
        """Search persons in ElasticSearch by specific query."""
        body = {
            "size": size, "from": (page - 1) * size,
            "query": {"match": {"full_name": {"query": query}}}
        }
        persons_data = await self.elastic.search(index="persons", body=body)
        return parse_objects(persons_data, Person)


def get_person_service(
        elastic: AsyncElasticsearch = Depends(get_elastic),
) -> PersonService:
    """Get a service for working with Person data."""
    return PersonService(elastic)
