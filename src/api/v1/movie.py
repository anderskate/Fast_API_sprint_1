from http import HTTPStatus
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_pagination import Page, add_pagination, paginate
from fastapi_pagination.bases import AbstractPage

from src.models.movie import Movie
from src.services.movie import MovieService, get_movie_service

router = APIRouter()


@router.get("/{movie_id}", response_model=Movie)
async def get_movie_details(
    movie_id: str, movie_service: MovieService = Depends(get_movie_service)
) -> Movie:
    """Represent movie details."""
    movie = await movie_service.get_by_id(movie_id)
    if not movie:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="movie not found")
    return movie


@router.get("/", response_model=Page[Movie])
async def get_movies(
    sort: Optional[str] = None,
    genres: Optional[List[str]] = Query(None),
    movie_service: MovieService = Depends(get_movie_service),
) -> AbstractPage[Movie]:
    """Represent all person movies with optional filters."""
    movies = await movie_service.get_all(sort, genres)
    return paginate(movies)


@router.get("/search", response_model=Page[Movie])
async def search_movies(
    query: str = None, movie_service: MovieService = Depends(get_movie_service)
) -> AbstractPage[Movie]:
    """Represent movies founded by specific query."""
    movies = await movie_service.search_movies(query)
    return paginate(movies)


add_pagination(router)
