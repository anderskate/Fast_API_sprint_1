import orjson
from uuid import UUID

from pydantic import BaseModel


class Genre(BaseModel):
    """Model to represent Genre objects."""
    id: UUID
    name: str

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson.dumps
