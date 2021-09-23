from .base import Base


class Genre(Base):
    """Model to represent Genre objects."""

    id: str
    name: str
