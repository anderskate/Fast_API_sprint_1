from fastapi import APIRouter

from src.api.v1 import movie

api_router = APIRouter()
api_router.include_router(movie.router)
