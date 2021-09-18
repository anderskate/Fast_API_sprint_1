import logging

import uvicorn
from fastapi import FastAPI
from fastapi.responses import ORJSONResponse
from elasticsearch import AsyncElasticsearch

from core import config
from core.logger import LOGGING
from db import elastic
from api.v1 import person, genre


app = FastAPI(
    title=config.PROJECT_NAME,
    docs_url="/api/openapi",
    openapi_url="/api/openapi.json",
    default_response_class=ORJSONResponse,
)


@app.on_event("startup")
async def startup():
    elastic.es = AsyncElasticsearch(
        hosts=[f'{config.ELASTIC_HOST}:{config.ELASTIC_PORT}']
    )


@app.on_event("shutdown")
async def shutdown():
    await elastic.es.close()


app.include_router(person.router, prefix='/api/v1/persons', tags=['persons'])
app.include_router(genre.router, prefix='/api/v1/genres', tags=['genres'])


if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        log_config=LOGGING,
        log_level=logging.DEBUG,
        reload=True,
    )
