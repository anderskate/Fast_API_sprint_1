from datetime import datetime

import redis


class RedisStateStorage:
    """Storage to store state updated time."""

    def __init__(self, redis_adapter: redis.Redis):
        self.redis_adapter = redis_adapter

    def save_state(self, state: datetime, key: str = 'start_from_ts'):
        formatted_state = state.isoformat()
        self.redis_adapter.set(key, formatted_state)

    def retrieve_state(self, key: str = 'start_from_ts'):
        raw_data = self.redis_adapter.get(key)
        if raw_data is None:
            return None
        formatted_state = datetime.fromisoformat(raw_data.decode('utf-8'))
        return formatted_state

    def delete_state(self, key: str = 'start_from_ts'):
        self.redis_adapter.delete(key)
