from fastapi import APIRouter

from src.api.v1 import genre, movie, person

api_router = APIRouter()
api_router.include_router(movie.router, prefix="/movies", tags=["movies"])
api_router.include_router(person.router, prefix="/persons", tags=["persons"])
api_router.include_router(genre.router, prefix="/genres", tags=["genres"])
