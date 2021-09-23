from http import HTTPStatus
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi_cache.decorator import cache

from src.models.movie import Movie
from src.services.movie import MovieService, get_movie_service

router = APIRouter()


@router.get("/search/", response_model=list[Movie])
@cache(expire=60 * 5)
async def search_movies(
        query: str,
        page: int = Query(1, ge=1),
        size: int = Query(100, ge=1, le=500),
        movie_service: MovieService = Depends(get_movie_service)
) -> list[Movie]:
    """Represent movies founded by specific query."""
    return await movie_service.search_movies(page, size, query)


@router.get("/{movie_id}", response_model=Movie)
@cache(expire=60 * 5)
async def get_movie_details(
        movie_id: str, movie_service: MovieService = Depends(get_movie_service)
) -> Movie:
    """Represent movie details."""
    movie = await movie_service.get_by_id(movie_id)
    if not movie:
        raise HTTPException(
            status_code=HTTPStatus.NOT_FOUND, detail="movie not found"
        )
    return movie


@router.get("", response_model=list[Movie])
@cache(expire=60 * 5)
async def get_movies(
        page: int = Query(1, ge=1),
        size: int = Query(100, ge=1, le=500),
        sort: Optional[str] = Query("imdb_rating", regex="-?imdb_rating$"),
        genres: Optional[list[str]] = Query(None),
        movie_service: MovieService = Depends(get_movie_service),
) -> list[Movie]:
    """Represent all movies with optional filters."""
    return await movie_service.get_all(page, size, sort, genres)
