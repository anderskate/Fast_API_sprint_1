from pydantic import BaseModel


class Genre(BaseModel):
    """Model to represent Genre objects."""

    id: str
    name: str
