from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_cache.decorator import cache

from src.models.movie import Movie
from src.models.person import Person
from src.services.person import PersonService, get_person_service

router = APIRouter()


@router.get("/{person_id}/movies/", response_model=list[Movie])
@cache(expire=60 * 5)
async def get_person_movies(
        person_id: str,
        page: int = Query(1, ge=1),
        size: int = Query(100, ge=1, le=500),
        person_service: PersonService = Depends(get_person_service)
) -> list[Movie]:
    """Represent all person movies."""
    person_movies = await person_service.get_person_movies(
        page, size, person_id
    )
    if person_movies is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="person not found"
        )
    return person_movies


@router.get("/{person_id}", response_model=Person)
@cache(expire=60 * 5)
async def get_person_details(
        person_id: str, person_service: PersonService = Depends(
            get_person_service
        )
) -> Person:
    """Represent Person details."""
    person = await person_service.get_by_id(person_id)

    if not person:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="person not found"
        )
    return person


@router.get("/", response_model=list[Person])
@cache(expire=60 * 5)
async def get_persons(
        search: str,
        page: int = Query(1, ge=1),
        size: int = Query(100, ge=1, le=500),
        person_service: PersonService = Depends(get_person_service)
) -> list[Person]:
    """Represent persons founded by specific search query."""
    return await person_service.search_persons(page, size, search)
