import orjson
from uuid import UUID
from datetime import date
from typing import Optional

from pydantic import BaseModel


class Person(BaseModel):
    """Model to represent Person objects."""
    id: UUID
    full_name: str
    birth_date: Optional[date]

    class Config:
        json_loads = orjson.loads
        json_dumps = orjson.dumps
