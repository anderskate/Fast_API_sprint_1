from aioredis import Redis
from typing import Optional

redis: Optional[Redis] = None


async def get_redis() -> Redis:
    """Get object with connection to Redis."""
    return redis
