from elasticsearch import AsyncElasticsearch
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from fastapi_cache import FastAPICache
from fastapi_cache.backends.redis import RedisBackend
import aioredis

from src.api.base_router import api_router
from src.core.config import settings
from src.db import elastic, redis

app = FastAPI(
    title=settings.PROJECT_NAME,
    docs_url="/api/openapi",
    openapi_url="/api/openapi.json",
    default_response_class=ORJSONResponse,
)

app.router.include_router(api_router, prefix="/api/v1")


@app.on_event("startup")
async def startup():
    elastic.es = AsyncElasticsearch(
        hosts=[f"{settings.ELASTIC_HOST}:{settings.ELASTIC_PORT}"]
    )
    redis.redis = aioredis.from_url(
        f"redis://{settings.REDIS_HOST}",
        encoding="utf8", decode_responses=True
    )
    FastAPICache.init(RedisBackend(redis.redis), prefix="fastapi-cache")


@app.on_event("shutdown")
async def shutdown():
    await elastic.es.close()
    await redis.redis.close()
