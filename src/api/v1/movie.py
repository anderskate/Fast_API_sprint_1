from http import HTTPStatus

from fastapi import APIRouter, Depends, HTTPException

from src.services.movie import MovieService, get_movie_service
from typing import Optional, List
from fastapi_pagination import Page, add_pagination, paginate
from fastapi_pagination.bases import AbstractPage
from fastapi import Query
from src.models.movie import Movie

router = APIRouter()


@router.get('/movies', response_model=Page[Movie])
async def get_movies(sort: Optional[str] = None, genres: Optional[List[str]] = Query(None),
                     movie_service: MovieService = Depends(get_movie_service)) -> AbstractPage[Movie]:
    movies = await movie_service.get_all(sort, genres)
    return paginate(movies)


@router.get('/movie/{movie_id}', response_model=Movie)
async def movie_details(movie_id: str, movie_service: MovieService = Depends(get_movie_service)) -> Movie:
    movie = await movie_service.get_by_id(movie_id)
    if not movie:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, default='movie not found')
    return movie


@router.get('/movies/search', response_model=Page[Movie])
async def search_movies(query: str = None,
                        movie_service: MovieService = Depends(get_movie_service)) -> AbstractPage[Movie]:
    movies = await movie_service.search_movies(query)
    return paginate(movies)


add_pagination(router)
