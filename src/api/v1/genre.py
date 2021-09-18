from http import HTTPStatus
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException

from src.models.genre import Genre
from src.services.genre import GenreService, get_genre_service

router = APIRouter()


@router.get("/{genre_id}", response_model=Genre)
async def get_genre_details(
    genre_id: str, genre_service: GenreService = Depends(get_genre_service)
) -> Genre:
    """Represent Genre details."""
    genre = await genre_service.get_by_id(genre_id)
    if not genre:
        raise HTTPException(status_code=HTTPStatus.NOT_FOUND, detail="genre not found")
    return genre


@router.get("/", response_model=List[Genre])
async def get_genres(
    genre_service: GenreService = Depends(get_genre_service),
) -> Optional[List[Genre]]:
    """Represent all genres."""
    genres = await genre_service.get_all()
    return genres
