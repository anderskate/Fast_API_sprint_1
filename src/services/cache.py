import json
from typing import  List
from aioredis import Redis
from fastapi import Depends

from core.config import CACHE_EXPIRE_IN_SECONDS
from db.redis import get_redis


class CacheService:
    """Service for saving and getting objects from Redis cache."""
    def __init__(self, redis: Redis):
        self.redis = redis

    async def get_objects_from_cache(self, objects_id: str, obj_cls):
        """Get Genres from Redis cache if exists."""
        objects_data = await self.redis.get(objects_id)
        if not objects_data:
            return None

        converted_json_objects = json.loads(objects_data)
        objects = [
            obj_cls(**object_data)
            for object_data in converted_json_objects
        ]
        return objects

    async def put_objects_to_cache(
            self, objects: List[dict], objects_id: str):
        """Save Genre data into redis cache."""
        await self.redis.set(
            objects_id,
            json.dumps(objects),
            expire=CACHE_EXPIRE_IN_SECONDS
        )

    async def get_obj_from_cache(self, obj_id: str, obj_cls):
        """Get Genre from Redis cache if exists."""
        obj_data = await self.redis.get(obj_id)
        if not obj_data:
            return None
        obj = obj_cls.parse_raw(obj_data)

        return obj

    async def put_obj_to_cache(self, obj):
        """Save Genre data into redis cache."""
        await self.redis.set(
            obj.id, obj.json(),
            expire=CACHE_EXPIRE_IN_SECONDS
        )


async def get_cache_service(redis: Redis = Depends(get_redis)) -> CacheService:
    return CacheService(redis)
