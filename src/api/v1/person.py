from typing import List
from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException
from fastapi_pagination import Page, add_pagination, paginate
from fastapi_pagination.bases import AbstractPage

from models.person import Person
from services.person import PersonService, get_person_service, PersonMovie


router = APIRouter()


@router.get('/{person_id}/movies/', response_model=List[PersonMovie])
async def get_person_movies(
        person_id: str,
        person_service: PersonService = Depends(get_person_service)
) -> List[PersonMovie]:
    """Represent all person movies."""
    person_movies = await person_service.get_person_movies(person_id)
    if person_movies is None:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='person not found'
        )
    return person_movies


@router.get('/search/', response_model=Page[Person])
async def search_persons(
        query: str,
        person_service: PersonService = Depends(get_person_service)
) -> AbstractPage[Person]:
    """Represent persons founded by specific query."""
    found_persons = await person_service.search_persons(query)
    return paginate(found_persons)


@router.get('/{person_id}', response_model=Person)
async def get_person_details(
        person_id: str,
        person_service: PersonService = Depends(get_person_service)) -> Person:
    """Represent Person details."""
    person = await person_service.get_by_id(person_id)

    if not person:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND,
            detail='person not found'
        )
    return person


add_pagination(router)
