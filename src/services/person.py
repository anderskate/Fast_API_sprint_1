from typing import List, Optional
from uuid import UUID

from elasticsearch import AsyncElasticsearch
from fastapi import Depends
from pydantic import BaseModel

from db.elastic import get_elastic
from models.person import Person
from .cache import CacheService, get_cache_service


class PersonMovie(BaseModel):
    """Model to represent movie in which the person took part."""

    id: UUID
    title: str
    imdb_rating: float


class PersonService:
    """Service for getting data by Person."""

    def __init__(self, elastic: AsyncElasticsearch, cache_service: CacheService):
        self.elastic = elastic
        self.cache_service = cache_service

    async def get_by_id(self, person_id: str) -> Optional[Person]:
        """Get person data by id."""
        person = await self.cache_service.get_obj_from_cache(person_id, Person)
        if person:
            return person

        person = await self._get_person_from_elastic(person_id)
        await self.cache_service.put_obj_to_cache(person)
        return person

    async def _get_person_from_elastic(self, person_id: str) -> Optional[Person]:
        """Get person data from ElasticSearch."""
        if not await self.elastic.exists("persons", person_id):
            return None
        person_data = await self.elastic.get("persons", person_id)
        return Person(**person_data["_source"])

    async def _get_person_movies_from_elastic(self, movie_ids: List[UUID]):
        """Get all person movies data from ElasticSearch."""
        search_body = {"query": {"ids": {"values": movie_ids}}}
        person_movies_data = await self.elastic.search(index="movies", body=search_body)

        person_movies = [
            PersonMovie(**movie_data["_source"])
            for movie_data in person_movies_data["hits"]["hits"]
        ]
        return person_movies

    async def get_person_movies(self, person_id: str) -> Optional[Person]:
        """Get all person movies data."""
        person = await self.cache_service.get_obj_from_cache(person_id, Person)
        if not person:
            person = await self._get_person_from_elastic(person_id)
        if not person:
            return None
        person_movie_ids = [movie.id for movie in person.related_movies]
        person_movies = await self._get_person_movies_from_elastic(person_movie_ids)
        return person_movies

    async def search_persons(self, query: str) -> Optional[List[Person]]:
        """Find persons by specific query."""
        found_persons = await self._search_persons_from_elastic(query)
        return found_persons

    async def _search_persons_from_elastic(self, query: str) -> Optional[List[Person]]:
        """Search persons in ElasticSearch by specific query."""
        search_query = {"query": {"match": {"full_name": {"query": query}}}}
        persons_data = await self.elastic.search(index="persons", body=search_query)
        persons = [
            Person(**person_data["_source"])
            for person_data in persons_data["hits"]["hits"]
        ]
        return persons


def get_person_service(
    elastic: AsyncElasticsearch = Depends(get_elastic),
    cache_service: CacheService = Depends(get_cache_service)
) -> PersonService:
    """Get a service for working with Person data."""
    return PersonService(elastic, cache_service)
