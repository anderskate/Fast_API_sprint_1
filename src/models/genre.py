import orjson
from pydantic import BaseModel


class Genre(BaseModel):
    """Model to represent Genre objects."""

    id: str
    name: str

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson.dumps
