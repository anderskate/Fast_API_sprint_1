from typing import Optional

from elasticsearch import AsyncElasticsearch


es: Optional[AsyncElasticsearch] = None


async def get_elastic() -> AsyncElasticsearch:
    """Get object with connection to ElasticSearch."""
    return es
