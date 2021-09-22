from operator import itemgetter
from typing import List, Optional, Type, Union

from pydantic import parse_obj_as

from src.models.movie import FIELDS_FOR_SEARCH, Movie, Person


def get_movies_sorting_for_elastic(sort_field: str) -> dict:
    return {
        "sort": {
            "imdb_rating": {"order": "asc" if sort_field == "imdb_rating" else "desc"}
        }
    }


def get_genres_filter_for_elastic(genres: List[str]) -> dict:
    should = [{"match": {"genres.id": genre} for genre in genres}]
    return {
        "query": {"nested": {"path": "genres", "query": {"bool": {"should": should}}}}
    }


def get_search_body_for_movies(
    query: str, fields_for_search: Optional[List[str]] = None
) -> dict:
    if fields_for_search is None:
        fields_for_search = FIELDS_FOR_SEARCH
    return {"query": {"multi_match": {"query": query, "fields": fields_for_search}}}


def parse_objects(doc: dict, schema: Type[Union[Movie, Person]]) -> List:
    if doc and doc.get("hits"):
        return parse_obj_as(
            List[schema], list(map(itemgetter("_source"), doc["hits"].get("hits", [])))
        )
    return []
